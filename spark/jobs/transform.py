import argparse
import glob
import os
import tempfile
from datetime import datetime

from google.cloud import storage
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Columns we keep from the raw TLC file (schemas vary slightly across years)
KEEP_COLUMNS = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance",
    "PULocationID", "DOLocationID",
    "payment_type", "fare_amount", "tip_amount",
    "tolls_amount", "total_amount",
]

MIN_FARE = 3.0            # NYC minimum metered fare
MAX_PASSENGERS = 6        # NYC yellow cab legal limit
MIN_DURATION_MIN = 1
MAX_DURATION_MIN = 180


def get_gcs_client():
    creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/opt/airflow/gcp/credentials.json")
    return storage.Client.from_service_account_json(creds)


def download_from_gcs(bucket_name: str, blob_name: str, local_path: str):
    blob = get_gcs_client().bucket(bucket_name).blob(blob_name)
    blob.download_to_filename(local_path)
    print(f"Downloaded gs://{bucket_name}/{blob_name} -> {local_path}")


def upload_to_gcs(local_path: str, bucket_name: str, blob_name: str):
    blob = get_gcs_client().bucket(bucket_name).blob(blob_name)
    blob.upload_from_filename(local_path)
    print(f"Uploaded {local_path} -> gs://{bucket_name}/{blob_name}")


def month_bounds(year: int, month: int):
    start = datetime(year, month, 1)
    end = datetime(year + (month == 12), month % 12 + 1, 1)
    return start, end


def clean_trips(df: DataFrame, year: int, month: int) -> DataFrame:
    """Pure cleaning logic (no I/O) so it can be unit tested."""
    start, end = month_bounds(year, month)

    df = df.select([c for c in KEEP_COLUMNS if c in df.columns])

    # Explicit types: raw files mix doubles/longs between years
    df = (df
          .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp"))
          .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast("timestamp"))
          .withColumn("passenger_count", F.col("passenger_count").cast("long"))
          .withColumn("PULocationID", F.col("PULocationID").cast("long"))
          .withColumn("DOLocationID", F.col("DOLocationID").cast("long"))
          .withColumn("payment_type", F.col("payment_type").cast("long"))
          .withColumn("trip_distance", F.col("trip_distance").cast("double"))
          .withColumn("fare_amount", F.col("fare_amount").cast("double"))
          .withColumn("tip_amount", F.col("tip_amount").cast("double"))
          .withColumn("tolls_amount", F.col("tolls_amount").cast("double"))
          .withColumn("total_amount", F.col("total_amount").cast("double")))

    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime",
                           "trip_distance", "fare_amount"])

    df = df.withColumn(
        "trip_duration_minutes",
        F.round((F.unix_timestamp("tpep_dropoff_datetime")
                 - F.unix_timestamp("tpep_pickup_datetime")) / 60.0, 2),
    )

    df = df.filter(
        (F.col("trip_distance") > 0)
        & (F.col("fare_amount") >= MIN_FARE)
        & F.col("passenger_count").between(1, MAX_PASSENGERS)
        & F.col("trip_duration_minutes").between(MIN_DURATION_MIN, MAX_DURATION_MIN)
        # TLC files contain a few trips dated in other months/years: keep only this month
        # so a rerun of the month replaces exactly this month's rows in BigQuery.
        & (F.col("tpep_pickup_datetime") >= F.lit(start))
        & (F.col("tpep_pickup_datetime") < F.lit(end))
    )

    return (df
            .withColumn("cost_per_mile", F.round(F.col("total_amount") / F.col("trip_distance"), 2))
            .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
            .withColumn("pickup_hour", F.hour("tpep_pickup_datetime")))


def transform(local_input: str, local_output_dir: str, year: int, month: int):
    """Clean with Spark and write ONE parquet file without collecting data to the driver."""
    spark = (SparkSession.builder
             .appName("nyc-taxi-transform")
             # BigQuery reads TIMESTAMP_MICROS reliably (Spark's default INT96 is legacy)
             .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df = clean_trips(spark.read.parquet(local_input), year, month)

    # coalesce(1) keeps a single output file for a simple GCS object layout
    # (~3M rows / month is small enough for one file)
    df.coalesce(1).write.mode("overwrite").parquet(local_output_dir)
    row_count = spark.read.parquet(local_output_dir).count()
    spark.stop()

    part_files = glob.glob(os.path.join(local_output_dir, "part-*.parquet"))
    if len(part_files) != 1:
        raise RuntimeError(f"Expected 1 output file, found {len(part_files)}")
    print(f"Transformed rows: {row_count:,}")
    return part_files[0], row_count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year",              type=int, required=True)
    parser.add_argument("--month",             type=int, required=True)
    parser.add_argument("--raw_bucket",        required=True)
    parser.add_argument("--processed_bucket",  required=True)
    args = parser.parse_args()

    year, month = args.year, args.month
    raw_blob = f"yellow/{year}/{month:02d}/yellow_tripdata_{year}-{month:02d}.parquet"
    processed_blob = f"yellow/{year}/{month:02d}/yellow_tripdata_{year}-{month:02d}_processed.parquet"

    with tempfile.TemporaryDirectory() as tmpdir:
        local_input = os.path.join(tmpdir, "raw.parquet")
        local_output_dir = os.path.join(tmpdir, "processed")

        download_from_gcs(args.raw_bucket, raw_blob, local_input)
        part_file, rows = transform(local_input, local_output_dir, year, month)
        upload_to_gcs(part_file, args.processed_bucket, processed_blob)

    print(f"Done — {rows:,} rows written to gs://{args.processed_bucket}/{processed_blob}")


if __name__ == "__main__":
    main()
