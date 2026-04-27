# NYC Taxi Data Pipeline 

> An end-to-end batch data pipeline that ingests, transforms and models 4.5M+ NYC taxi trips monthly using a modern open-source data stack.

---

## Architecture

```
NYC TLC Dataset (Public)
        │
        ▼
┌──────────────────┐
│   GCS Raw Bucket │  ← Landing zone (raw Parquet)
└──────────────────┘
        │
        ▼
┌──────────────────┐
│   Apache Spark   │  ← Clean, cast types, engineer features
└──────────────────┘
        │
        ▼
┌────────────────────────┐
│  GCS Processed Bucket  │  ← Clean Parquet, partitioned by date
└────────────────────────┘
        │
        ▼
┌──────────────────────┐
│   BigQuery Raw       │  ← nyc_taxi_raw.yellow_taxi
└──────────────────────┘
        │
        ▼
┌──────────────────┐
│       dbt        │  ← staging → intermediate → marts
└──────────────────┘
        │
        ▼
┌──────────────────────┐
│   BigQuery Dev/Prod  │  ← fct_trips, stg_yellow_taxi
└──────────────────────┘
        │
        ▼
┌──────────────────┐
│  Looker Studio   │  ← Business dashboard
└──────────────────┘

All tasks orchestrated and scheduled by Apache Airflow
```

---

## What This Project Does

Every month this pipeline automatically:

1. **Ingests** raw NYC TLC trip data (Parquet files) from the public dataset
2. **Cleans** 4.5M+ rows using PySpark — removes invalid trips, fixes data types, engineers features
3. **Loads** processed data into BigQuery via Google Cloud Storage
4. **Models** the data using dbt — builds a star schema with automated tests and documentation
5. **Visualises** business metrics in a Looker Studio dashboard

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Orchestration | Apache Airflow | 2.8.1 |
| Processing | Apache Spark / PySpark | 3.5.1 |
| Storage | Google Cloud Storage | — |
| Data Warehouse | Google BigQuery | — |
| Transformation | dbt Core + dbt-bigquery | 1.7.0 |
| Dashboard | Looker Studio | — |
| Infrastructure | Docker + Docker Compose | — |
| Language | Python | 3.11 |
| Cloud | Google Cloud Platform | — |

---

## Pipeline Results

| Metric | Value |
|---|---|
| Trips processed (monthly) | **4,539,360** |
| Total revenue modelled | **$132,600,000+** |
| Credit card trips | **87.7%** |
| Cash trips | **11.2%** |
| Average fare | **$19.62** |
| Average trip distance | **3.4 miles** |
| Average trip duration | **18 minutes** |
| Average tip | **$3.64** |

---

## Project Structure

```
nyc-taxi-pipeline/
│
├── airflow/
│   ├── dags/
│   │   └── nyc_taxi_pipeline.py      ← Main DAG: ingest → transform → load → dbt
│   ├── plugins/
│   └── logs/
│
├── spark/
│   └── jobs/
│       ├── ingest_raw.py             ← Download TLC Parquet → GCS raw bucket
│       ├── transform.py              ← Clean + feature engineer → GCS processed
│       └── load_bq.py                ← Load processed Parquet → BigQuery table
│
├── dbt/
│   ├── profiles.yml                  ← BigQuery connection config
│   └── nyc_taxi/
│       ├── dbt_project.yml
│       └── models/
│           ├── staging/
│           │   ├── stg_yellow_taxi.sql   ← Clean column names + filter bad rows
│           │   └── schema.yml            ← Source definition + data quality tests
│           └── marts/
│               └── fct_trips.sql         ← Final fact table with business metrics
│
├── gcp/
│   └── credentials.json              ← GCP service account key (gitignored)
│
├── Dockerfile.airflow                ← Custom image: Airflow + Java 17 + Spark 3.5
├── docker-compose.yml                ← Airflow + Spark + Postgres services
├── .env.example                      ← Environment variable template
└── README.md
```

---

## Data Model

```
Source
└── nyc_taxi_raw.yellow_taxi          ← Raw data loaded by Spark (4.5M rows/month)

Staging (views)
└── nyc_taxi_dev.stg_yellow_taxi      ← Renamed columns, filtered invalid rows

Marts (tables)
└── nyc_taxi_dev.fct_trips            ← Fact table with:
        ├── trip_id (surrogate key)
        ├── pickup_datetime / dropoff_datetime
        ├── passenger_count
        ├── trip_distance
        ├── pickup_location_id / dropoff_location_id
        ├── fare_amount / tip_amount / total_amount
        ├── trip_duration_minutes
        ├── cost_per_mile
        └── payment_type_desc         ← Human-readable: Credit Card, Cash, etc.
```

---

## Airflow DAG

The `nyc_taxi_batch_pipeline` DAG runs on the 1st of every month at 6am.

```
ingest_raw_to_gcs → transform_to_processed → load_to_bigquery
```

| Task | Description | Timeout |
|---|---|---|
| `ingest_raw_to_gcs` | Downloads TLC Parquet, uploads to GCS raw bucket | 30 min |
| `transform_to_processed` | PySpark cleaning job, writes to GCS processed bucket | 30 min |
| `load_to_bigquery` | Loads processed Parquet into BigQuery table | 30 min |

---

## PySpark Transformations

The `transform.py` job applies these cleaning rules:

| Rule | Reason |
|---|---|
| Drop null pickup/dropoff times | Cannot calculate trip duration |
| Filter `fare_amount > 0` | Negative fares = data corruption |
| Filter `trip_distance > 0` | Zero distance = GPS error or cancelled |
| Filter `passenger_count` between 1–6 | NYC taxi legal limit |
| Filter `trip_duration_minutes` between 1–180 | Remove GPS errors and outliers |
| Filter `fare_amount >= 3.0` | Below NYC minimum fare |
| Cast types explicitly | Raw data has inconsistent types |

Engineered features added:
- `trip_duration_minutes` — calculated from pickup/dropoff timestamps
- `pickup_date` — extracted date for partitioning
- `pickup_hour` — extracted hour for hourly analysis
- `cost_per_mile` — derived fare efficiency metric

---

## dbt Tests

dbt runs these data quality tests after every transformation:

```yaml
- name: pickup_datetime
  tests: [not_null]

- name: fare_amount
  tests: [not_null]

- name: passenger_count
  tests: [not_null]
```

If any test fails, the pipeline stops and Airflow marks the run as failed.

---

## How To Run

### Prerequisites

- Docker Desktop installed and running
- GCP account (free tier is enough — $300 free credits)
- GCP service account with roles: `BigQuery Admin`, `Storage Admin`

### GCP Setup

1. Create a GCP project at [console.cloud.google.com](https://console.cloud.google.com)
2. Enable APIs: BigQuery API, Cloud Storage API, IAM API
3. Create a service account → download JSON key → rename to `credentials.json`
4. Create two GCS buckets: `nyc-taxi-raw-yourname`, `nyc-taxi-processed-yourname`
5. Create three BigQuery datasets: `nyc_taxi_raw`, `nyc_taxi_dev`, `nyc_taxi_prod` (region: `us-central1`)

### Local Setup

```bash
# Clone the repo
git clone https://github.com/zakariaaouass/nyc-taxi-pipeline.git
cd nyc-taxi-pipeline/nyc-taxi-pipeline

# Create environment file
cp .env.example .env
```

Edit `.env` with your values:

```env
GCP_PROJECT_ID=your-project-id
GCP_REGION=us-central1
RAW_BUCKET=nyc-taxi-raw-yourname
PROCESSED_BUCKET=nyc-taxi-processed-yourname
FERNET_KEY=your-fernet-key-here
AIRFLOW_UID=50000
```

```bash
# Add GCP credentials
cp /path/to/credentials.json gcp/credentials.json

# Build the custom Docker image (Airflow + Java 17 + Spark 3.5)
# Takes 5-10 minutes on first build
docker-compose build

# Initialise Airflow database
docker-compose up airflow-init

# Start all services
docker-compose up -d

# Verify everything is running
docker-compose ps
```

### Run the Pipeline

1. Open Airflow at `http://localhost:8080` — login: `admin / admin`
2. Find `nyc_taxi_batch_pipeline` → toggle **ON**
3. Click **▶ Trigger DAG**
4. Watch all 3 tasks turn green (~15 minutes total)
5. Verify data in BigQuery:

```sql
SELECT
  COUNT(*)                     AS total_trips,
  ROUND(AVG(fare_amount), 2)   AS avg_fare,
  SUM(total_amount)            AS total_revenue
FROM `your-project.nyc_taxi_raw.yellow_taxi`
```

### Run dbt Models

```bash
# Run all models
docker-compose exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt/nyc_taxi && \
   ~/.local/bin/dbt run --profiles-dir /opt/airflow/dbt --no-version-check"

# Run data quality tests
docker-compose exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt/nyc_taxi && \
   ~/.local/bin/dbt test --profiles-dir /opt/airflow/dbt --no-version-check"
```
##Key Engineering Decisions
Why PySpark instead of Pandas?
The dataset has 3M+ rows per month. Pandas loads everything into RAM and crashes. PySpark processes in parallel chunks and handles any data volume.
Why GCS as intermediate storage?
Decouples Spark processing from BigQuery loading. If the BigQuery load fails, the clean Parquet is already in GCS — no need to re-run the expensive Spark job.
Why dbt for transformations?
Separates raw ingestion from business logic. Tests catch data quality issues before they reach dashboards. Column-level lineage is auto-documented. dbt run replaces hundreds of lines of manual SQL.
Why Docker Compose instead of managed services?
Zero cloud spend during development. Airflow and Spark run locally — only GCS and BigQuery are cloud services. Monthly cost: under $1.
Why incremental dbt models?
The fct_trips table uses is_incremental() logic — each month only appends new data instead of rebuilding the entire table. Saves BigQuery processing costs.






