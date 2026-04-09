import argparse
import os
import requests
from google.cloud import storage
from pyspark.sql import SparkSession


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


def count_rows(local_path: str) -> int:
    spark = SparkSession.builder.appName("nyc-taxi-ingest").getOrCreate()
    count = spark.read.parquet(local_path).count()
    spark.stop()
    return count


def upload_to_gcs(local_path: str, bucket: str, year: int, month: int):
    """Upload parquet file to GCS using the Python client — no Spark GCS connector needed."""
    creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/opt/airflow/gcp/credentials.json")
    print(f"Using credentials at: {creds}")

    client = storage.Client.from_service_account_json(creds)
    bucket_obj = client.bucket(bucket)

    blob_name = f"yellow/{year}/{month:02d}/yellow_tripdata_{year}-{month:02d}.parquet"
    blob = bucket_obj.blob(blob_name)
    blob.upload_from_filename(local_path)
    print(f"Uploaded to gs://{bucket}/{blob_name}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year",   type=int, required=True)
    parser.add_argument("--month",  type=int, required=True)
    parser.add_argument("--bucket", required=True)
    args = parser.parse_args()

    local_path = download_parquet(args.year, args.month)
    upload_to_gcs(local_path, args.bucket, args.year, args.month)
    rows = count_rows(local_path)
    print(f"Done — {rows:,} rows uploaded for {args.year}-{args.month:02d}")


if __name__ == "__main__":
    main()
