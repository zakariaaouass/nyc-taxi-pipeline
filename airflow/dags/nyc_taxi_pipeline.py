import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Configuration comes from .env (passed in by docker-compose); defaults keep old setups working
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "nyc-taxi-pipeline-492610")
REGION = os.environ.get("GCP_REGION", "us-central1")
RAW_BUCKET = os.environ.get("RAW_BUCKET", "nyc-taxi-raw-zakaria")
PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET", "nyc-taxi-processed-zakaria")
DBT_TARGET = os.environ.get("DBT_TARGET", "dev")

# NYC TLC publishes data ~2 months after the fact, so every run processes the month
# two months before the logical date.
YEAR = "{{ (data_interval_start - macros.dateutil.relativedelta.relativedelta(months=2)).year }}"
MONTH = "{{ (data_interval_start - macros.dateutil.relativedelta.relativedelta(months=2)).month }}"

default_args = {
    "owner": "zakaria",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="nyc_taxi_batch_pipeline",
    default_args=default_args,
    description="Monthly NYC Taxi batch pipeline: ingest -> Spark clean -> BigQuery -> dbt",
    schedule_interval="0 6 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["nyc-taxi", "batch"],
) as dag:

    # Spark runs in local mode inside the Airflow container (SPARK_HOME=/opt/spark).
    # The GCS/Guava version conflict is solved in Dockerfile.airflow by swapping the Guava jar.
    ingest_raw = BashOperator(
        task_id="ingest_raw_to_gcs",
        bash_command=f"""
            spark-submit --master local[2] /opt/airflow/spark/jobs/ingest_raw.py \
              --year {YEAR} --month {MONTH} --bucket {RAW_BUCKET}
        """,
        execution_timeout=timedelta(minutes=30),
    )

    transform = BashOperator(
        task_id="transform_to_processed",
        bash_command=f"""
            spark-submit --master local[2] /opt/airflow/spark/jobs/transform.py \
              --year {YEAR} --month {MONTH} \
              --raw_bucket {RAW_BUCKET} --processed_bucket {PROCESSED_BUCKET}
        """,
        execution_timeout=timedelta(minutes=30),
    )

    load_bq = BashOperator(
        task_id="load_to_bigquery",
        bash_command=f"""
            python3 /opt/airflow/spark/jobs/load_bq.py \
              --year {YEAR} --month {MONTH} \
              --processed_bucket {PROCESSED_BUCKET} \
              --project {PROJECT_ID} --dataset nyc_taxi_raw --table yellow_taxi \
              --location {REGION}
        """,
        execution_timeout=timedelta(minutes=30),
    )

    # dbt build = seeds + models + tests in dependency order.
    # A failing test fails this task, so bad data never reaches the marts silently.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"""
            cd /opt/airflow/dbt/nyc_taxi && \
            dbt build --profiles-dir /opt/airflow/dbt --target {DBT_TARGET} --no-version-check
        """,
        execution_timeout=timedelta(minutes=30),
    )

    ingest_raw >> transform >> load_bq >> dbt_build
