from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "you",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="nyc_taxi_batch_pipeline",
    default_args=default_args,
    description="Monthly NYC Taxi batch pipeline",
    schedule_interval="0 6 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["nyc-taxi", "batch", "project-1"],
) as dag:

    # NYC TLC publishes data ~2 months after the fact, so we offset by -2 months
    # to ensure we always fetch data that has actually been published.
    #
    # userClassPathFirst=true fixes Guava version conflict: GCS connector needs
    # Guava 31+ but Spark bundles an older version that would otherwise win.
    ingest_raw = BashOperator(
        task_id="ingest_raw_to_gcs",
        bash_command="""
            /opt/spark/bin/spark-submit \
              --master local[2] \
              /opt/airflow/spark/jobs/ingest_raw.py \
              --year {{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=2)).year }} \
              --month {{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=2)).month }} \
              --bucket nyc-taxi-raw-zakaria
        """,
        execution_timeout=timedelta(minutes=30),
    )

    transform = BashOperator(
        task_id="transform_to_processed",
        bash_command="""
            /opt/spark/bin/spark-submit \
              --master local[2] \
              /opt/airflow/spark/jobs/transform.py \
              --year {{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=2)).year }} \
              --month {{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=2)).month }} \
              --raw_bucket nyc-taxi-raw-zakaria \
              --processed_bucket nyc-taxi-processed-zakaria
        """,
        execution_timeout=timedelta(minutes=30),
    )

    load_bq = BashOperator(
        task_id="load_to_bigquery",
        bash_command="""
            python3 /opt/airflow/spark/jobs/load_bq.py \
              --year {{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=2)).year }} \
              --month {{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=2)).month }} \
              --processed_bucket nyc-taxi-processed-zakaria \
              --project nyc-taxi-pipeline-492610 \
              --dataset nyc_taxi_raw \
              --table yellow_taxi
        """,
        execution_timeout=timedelta(minutes=30),
    )

    ingest_raw >> transform >> load_bq