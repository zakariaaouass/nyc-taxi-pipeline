import argparse
import os
from datetime import datetime

from google.cloud import bigquery
from google.oauth2 import service_account


def get_bq_client(project: str):
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/opt/airflow/gcp/credentials.json")
    creds = service_account.Credentials.from_service_account_file(creds_path)
    return bigquery.Client(project=project, credentials=creds)


def month_bounds(year: int, month: int):
    start = datetime(year, month, 1)
    end = datetime(year + (month == 12), month % 12 + 1, 1)
    return start, end


def load_to_bigquery(processed_bucket: str, year: int, month: int,
                     project: str, dataset: str, table: str, location: str):
    """Idempotent monthly load: rerunning a month replaces it instead of duplicating it."""
    gcs_uri = (f"gs://{processed_bucket}/yellow/{year}/{month:02d}/"
               f"yellow_tripdata_{year}-{month:02d}_processed.parquet")
    table_id = f"{project}.{dataset}.{table}"
    client = get_bq_client(project)

    ds = bigquery.Dataset(f"{project}.{dataset}")
    ds.location = location          # must match the dbt profile location
    client.create_dataset(ds, exists_ok=True)
    print(f"Dataset {project}.{dataset} ready ({location})")

    # 1) Remove any rows already loaded for this month (transform.py keeps only this month's trips)
    start, end = month_bounds(year, month)
    try:
        client.get_table(table_id)
        job = client.query(
            f"DELETE FROM `{table_id}` "
            "WHERE tpep_pickup_datetime >= @start AND tpep_pickup_datetime < @end",
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ScalarQueryParameter("start", "TIMESTAMP", start),
                bigquery.ScalarQueryParameter("end", "TIMESTAMP", end),
            ]),
            location=location,
        )
        job.result()
        print(f"Deleted {job.num_dml_affected_rows or 0:,} existing rows for {year}-{month:02d}")
    except Exception as exc:  # table does not exist yet on the very first run
        if "Not found" not in str(exc):
            raise
        print(f"{table_id} does not exist yet, it will be created")

    # 2) Append the month
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
    )
    print(f"Loading {gcs_uri} -> {table_id}")
    client.load_table_from_uri(gcs_uri, table_id, job_config=job_config, location=location).result()

    print(f"Done — {client.get_table(table_id).num_rows:,} total rows now in {table_id}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year",             type=int, required=True)
    parser.add_argument("--month",            type=int, required=True)
    parser.add_argument("--processed_bucket", required=True)
    parser.add_argument("--project",          required=True)
    parser.add_argument("--dataset",          required=True)
    parser.add_argument("--table",            required=True)
    parser.add_argument("--location",         default=os.environ.get("GCP_REGION", "us-central1"))
    args = parser.parse_args()

    load_to_bigquery(args.processed_bucket, args.year, args.month,
                     args.project, args.dataset, args.table, args.location)


if __name__ == "__main__":
    main()
