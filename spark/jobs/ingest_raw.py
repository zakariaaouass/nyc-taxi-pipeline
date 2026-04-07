import argparse
import os
import requests
from pyspark.sql import SparkSession



def get_spark():
    creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/opt/gcp/credentials.json")

    print(f"🔑 Using credentials at: {creds}")

    return (
        SparkSession.builder
        .appName("nyc-taxi-ingest")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", creds)
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .getOrCreate()
    )

def download_parquet(year: int, month: int) -> str:
    url = (
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/"
        f"yellow_tripdata_{year}-{month:02d}.parquet"
    )
    local_path = f"/tmp/yellow_{year}_{month:02d}.parquet"
    print(f"Downloading: {url}")
    r = requests.get(url, stream=True, timeout=120)
    r.raise_for_status()
    with open(local_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Saved locally to {local_path}")
    return local_path


def upload_to_gcs(spark, local_path: str, bucket: str, year: int, month: int):
    df = spark.read.parquet(local_path)
    row_count = df.count()
    print(f"Loaded {row_count:,} rows")

    gcs_path = f"gs://{bucket}/yellow/{year}/{month:02d}/"
    df.write.mode("overwrite").parquet(gcs_path)
    print(f"Uploaded to {gcs_path}")
    return row_count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year",   type=int, required=True)
    parser.add_argument("--month",  type=int, required=True)
    parser.add_argument("--bucket", required=True)
    args = parser.parse_args()

    spark = get_spark()
    local_path = download_parquet(args.year, args.month)
    rows = upload_to_gcs(spark, local_path, args.bucket, args.year, args.month)
    print(f"Done — {rows:,} rows uploaded for {args.year}-{args.month:02d}")
    spark.stop()


if __name__ == "__main__":
    main()