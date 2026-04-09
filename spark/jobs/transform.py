import argparse
import os
import tempfile
from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_gcs_client():
    creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/opt/airflow/gcp/credentials.json")
    return storage.Client.from_service_account_json(creds)


def download_from_gcs(bucket_name: str, blob_name: str, local_path: str):
    client = get_gcs_client()
    blob = client.bucket(bucket_name).blob(blob_name)
    blob.download_to_filename(local_path)
    print(f"Downloaded gs://{bucket_name}/{blob_name} -> {local_path}")


def upload_to_gcs(local_path: str, bucket_name: str, blob_name: str):
    client = get_gcs_client()
    blob = client.bucket(bucket_name).blob(blob_name)
    blob.upload_from_filename(local_path)
    print(f"Uploaded {local_path} -> gs://{bucket_name}/{blob_name}")


def transform(local_input: str, local_output: str):
    spark = SparkSession.builder.appName("nyc-taxi-transform").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(local_input)

    # Keep only needed columns (handle varying schemas across years)
    keep = [
        "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance",
        "PULocationID", "DOLocationID",
        "payment_type", "fare_amount", "tip_amount",
        "tolls_amount", "total_amount",
    ]
    existing = [c for c in keep if c in df.columns]
    df = df.select(existing)

    # Clean: drop nulls on critical columns
    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime",
                            "trip_distance", "fare_amount"])

    # Filter invalid rows
    df = df.filter(
        (F.col("trip_distance") > 0) &
        (F.col("fare_amount") > 0) &
        (F.col("passenger_count") > 0)
    )

    # Add derived columns
    df = df.withColumn(
        "trip_duration_minutes",
        F.round(
            (F.unix_timestamp("tpep_dropoff_datetime") -
             F.unix_timestamp("tpep_pickup_datetime")) / 60.0,
            2
        )
    ).filter(F.col("trip_duration_minutes") > 0)

    df = df.withColumn(
        "cost_per_mile",
        F.round(F.col("total_amount") / F.col("trip_distance"), 2)
    )

    row_count = df.count()
    print(f"Transformed rows: {row_count:,}")

    # Write as a single parquet file via pandas (avoids multi-part directory issue)
    df.toPandas().to_parquet(local_output, index=False)
    spark.stop()
    return row_count


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
        local_input  = os.path.join(tmpdir, "raw.parquet")
        local_output = os.path.join(tmpdir, "processed.parquet")

        download_from_gcs(args.raw_bucket, raw_blob, local_input)
        rows = transform(local_input, local_output)
        upload_to_gcs(local_output, args.processed_bucket, processed_blob)

    print(f"Done — {rows:,} rows written to gs://{args.processed_bucket}/{processed_blob}")


if __name__ == "__main__":
    main()
