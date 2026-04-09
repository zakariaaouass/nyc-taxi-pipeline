import argparse
import os
from google.cloud import bigquery


def get_bq_client(project: str):
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/opt/airflow/gcp/credentials.json")
    from google.oauth2 import service_account
    creds = service_account.Credentials.from_service_account_file(creds_path)
    return bigquery.Client(project=project, credentials=creds)


def load_to_bigquery(
    processed_bucket: str,
    year: int,
    month: int,
    project: str,
    dataset: str,
    table: str,
):
    gcs_uri = (
        f"gs://{processed_bucket}/yellow/{year}/{month:02d}/"
        f"yellow_tripdata_{year}-{month:02d}_processed.parquet"
    )
    table_id = f"{project}.{dataset}.{table}"

    client = get_bq_client(project)

    # Create dataset if it doesn't exist
    ds_ref = bigquery.Dataset(f"{project}.{dataset}")
    ds_ref.location = "US"
    client.create_dataset(ds_ref, exists_ok=True)
    print(f"Dataset {project}.{dataset} ready")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )

    print(f"Loading {gcs_uri} -> {table_id}")
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()  # Wait for job to complete

    table_ref = client.get_table(table_id)
    print(f"Done — {table_ref.num_rows:,} total rows now in {table_id}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year",             type=int, required=True)
    parser.add_argument("--month",            type=int, required=True)
    parser.add_argument("--processed_bucket", required=True)
    parser.add_argument("--project",          required=True)
    parser.add_argument("--dataset",          required=True)
    parser.add_argument("--table",            required=True)
    args = parser.parse_args()

    load_to_bigquery(
        processed_bucket=args.processed_bucket,
        year=args.year,
        month=args.month,
        project=args.project,
        dataset=args.dataset,
        table=args.table,
    )


if __name__ == "__main__":
    main()
