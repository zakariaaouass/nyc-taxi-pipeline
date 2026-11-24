# NYC Taxi Data Pipeline 

<img width="3260" height="970" alt="Untitled Diagram drawio" src="https://github.com/user-attachments/assets/2ca10f19-0185-4cc6-b23a-2256299278b8" />


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
│  GCS Processed Bucket  │  ← Clean Parquet, one file per month
└────────────────────────┘
        │
        ▼
┌──────────────────────┐
│   BigQuery Raw       │  ← nyc_taxi_raw.yellow_taxi
└──────────────────────┘
        │
        ▼
┌──────────────────┐
│       dbt        │  ← staging → star schema (fact + dims) + tests
└──────────────────┘
        │
        ▼
┌──────────────────────┐
│   BigQuery Dev/Prod  │  ← fct_trips, dim_location, dim_date, dim_payment_type
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
3. **Loads** processed data into BigQuery via Google Cloud Storage — idempotently, so re-running a month replaces it
4. **Models** the data using dbt — an incremental fact table, three dimensions, and tests that fail the pipeline on bad data
5. **Visualises** business metrics in a Looker Studio dashboard

---

## Tech Stack

| Layer | Technology | Version |
|---|---|---|
| Orchestration | Apache Airflow | 2.8.1 |
| Processing | Apache Spark / PySpark | 3.5.1 |
| Storage | Google Cloud Storage | — |
| Data Warehouse | Google BigQuery | — |
| Transformation | dbt Core + dbt-bigquery | 1.7.x |
| Dashboard | Looker Studio | — |
| Infrastructure | Docker + Docker Compose | — |
| Language | Python | 3.11 |
| Testing | pytest + dbt tests | — |
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


> Results from a monthly run of the original pipeline. The stricter cleaning rules below remove a few more invalid trips, so counts change slightly after a re-run.

---

## Project Structure

```
nyc-taxi-pipeline/
│
├── airflow/
│   └── dags/
│       └── nyc_taxi_pipeline.py      ← DAG: ingest → transform → load → dbt build
│
├── spark/
│   ├── jobs/
│   │   ├── ingest_raw.py             ← Download TLC Parquet → GCS raw bucket
│   │   ├── transform.py              ← Clean + feature engineer with Spark → GCS processed
│   │   └── load_bq.py                ← Idempotent monthly load → BigQuery
│   └── tests/
│       └── test_transform.py         ← pytest unit tests for the cleaning rules
│
├── dbt/
│   ├── profiles.yml                  ← BigQuery connection (project/region from .env)
│   └── nyc_taxi/
│       ├── dbt_project.yml
│       ├── seeds/
│       │   ├── taxi_zone_lookup.csv  ← Official TLC zone lookup (265 zones)
│       │   └── payment_types.csv     ← TLC payment codes
│       └── models/
│           ├── staging/
│           │   ├── stg_yellow_taxi.sql   ← Rename, dedupe, surrogate key
│           │   └── schema.yml            ← Source + tests
│           └── marts/
│               ├── fct_trips.sql         ← Incremental fact table
│               ├── dim_location.sql
│               ├── dim_payment_type.sql
│               ├── dim_date.sql
│               └── schema.yml            ← Keys + relationship tests
│
├── gcp/
│   └── credentials.json              ← GCP service account key (gitignored)
│
├── Dockerfile.airflow                ← Airflow + Java 17 + Spark 3.5 + dbt-bigquery
├── docker-compose.yml                ← Airflow + Postgres (+ optional Spark standalone)
├── requirements-dev.txt              ← pyspark + pytest for local tests
├── .env.example                      ← Environment variable template
└── README.md
```

---

## Data Model (star schema)

```
                 ┌──────────────────┐
                 │    dim_date      │
                 │  date_day (PK)   │
                 └────────┬─────────┘
                          │
┌──────────────────┐   ┌──┴────────────────────────┐   ┌──────────────────────┐
│  dim_location    │   │        fct_trips          │   │  dim_payment_type    │
│ location_id (PK) ├───┤ trip_key (PK)             ├───┤ payment_type (PK)    │
│ borough, zone,   │   │ pickup_date (FK)          │   │ payment_type_desc    │
│ service_zone     │   │ pickup/dropoff_location_id│   └──────────────────────┘
└──────────────────┘   │ payment_type (FK)         │
                       │ passenger_count, distance │
                       │ duration, fare, tip, tolls│
                       │ total_amount, cost/mile   │
                       └───────────────────────────┘
```

| Model | Materialization | Notes |
|---|---|---|
| `stg_yellow_taxi` | view | snake_case names, exact duplicates removed, `trip_key` = MD5 of the trip's attributes |
| `fct_trips` | **incremental** (merge on `trip_key`) | partitioned by month on `pickup_datetime`, clustered by location and payment type |
| `dim_location` | table | from the official TLC zone lookup |
| `dim_payment_type` | table | from the TLC data dictionary |
| `dim_date` | table | one row per day in the data |

---

## Airflow DAG

The `nyc_taxi_batch_pipeline` DAG runs on the 1st of every month at 6am and processes the month **two months earlier** (TLC publishes data with a ~2 month delay).

```
ingest_raw_to_gcs → transform_to_processed → load_to_bigquery → dbt_build
```

| Task | Description | Timeout |
|---|---|---|
| `ingest_raw_to_gcs` | Downloads TLC Parquet, uploads to GCS raw bucket | 30 min |
| `transform_to_processed` | PySpark cleaning job, writes one Parquet file to the processed bucket | 30 min |
| `load_to_bigquery` | Replaces that month in `nyc_taxi_raw.yellow_taxi` (delete + append) | 30 min |
| `dbt_build` | Seeds, models and tests in dependency order | 30 min |

Buckets, project, region and dbt target come from `.env`, so nothing is hard-coded in the DAG.

---

## PySpark Transformations

`transform.py` keeps the cleaning logic in a pure function (`clean_trips`) so it is unit tested without GCP.

| Rule | Reason |
|---|---|
| Cast every column to an explicit type | Raw files mix doubles/longs between years |
| Drop nulls on pickup/dropoff time, distance, fare | Cannot compute duration or price metrics |
| `trip_distance > 0` | Zero distance = GPS error or cancelled trip |
| `fare_amount >= 3.0` | Below the NYC minimum fare |
| `passenger_count` between 1 and 6 | NYC yellow cab legal limit |
| `trip_duration_minutes` between 1 and 180 | Remove GPS errors and outliers |
| Pickup inside the processed month | TLC files contain stray trips from other months |

Engineered features: `trip_duration_minutes`, `cost_per_mile`, `pickup_date`, `pickup_hour`.

The data stays in Spark end to end: it is written with `coalesce(1)` as a single Parquet file, never collected to the driver.

---

## Data Quality

**dbt tests** (run by `dbt build` inside the DAG; a failing test fails the `dbt_build` task):

| Model | Tests |
|---|---|
| `stg_yellow_taxi` | `trip_key` unique + not null, not null on timestamps, fare and passengers, `payment_type` accepted values 0–6 |
| `fct_trips` | `trip_key` unique + not null, relationships to `dim_date`, `dim_location` (pickup and dropoff) and `dim_payment_type` |
| dimensions | primary keys unique + not null |

**Unit tests** for the Spark rules:

```bash
pip install -r requirements-dev.txt
pytest spark/tests -q
```

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
5. Create three BigQuery datasets in your region (e.g. `us-central1`): `nyc_taxi_raw`, `nyc_taxi_dev`, `nyc_taxi_prod`

### Local Setup

```bash
git clone https://github.com/zakariaaouass/nyc-taxi-pipeline.git
cd nyc-taxi-pipeline
cp .env.example .env
```

Edit `.env` with your values (project, region, bucket names, `FERNET_KEY`, `AIRFLOW_SECRET_KEY`).

```bash
# Add GCP credentials
cp /path/to/credentials.json gcp/credentials.json

# Build the custom image (Airflow + Java 17 + Spark 3.5 + dbt) — 5-10 minutes the first time
docker-compose build

# Initialise Airflow and start the services
docker-compose up airflow-init
docker-compose up -d
docker-compose ps
```

### Run the Pipeline

1. Open Airflow at `http://localhost:8080` — login: `admin / admin`
2. Turn **ON** `nyc_taxi_batch_pipeline` and click **▶ Trigger DAG**
3. Watch the 4 tasks turn green
4. Check BigQuery:

```sql
SELECT
  z.borough,
  COUNT(*)                         AS trips,
  ROUND(AVG(f.fare_amount), 2)     AS avg_fare,
  ROUND(SUM(f.total_amount), 0)    AS revenue
FROM `your-project.nyc_taxi_dev.fct_trips` f
JOIN `your-project.nyc_taxi_dev.dim_location` z
  ON f.pickup_location_id = z.location_id
GROUP BY z.borough
ORDER BY trips DESC
```

### Run dbt manually

```bash
docker-compose exec airflow-scheduler bash -c \
  "cd /opt/airflow/dbt/nyc_taxi && dbt build --profiles-dir /opt/airflow/dbt --target dev"
```

### Upgrading from the first version

Column types in `nyc_taxi_raw.yellow_taxi` changed (explicit casts, two new columns), and older loads could contain duplicated months. Drop `nyc_taxi_raw.yellow_taxi` and `nyc_taxi_dev.fct_trips` once, then re-run the months you need.

---

## Key Engineering Decisions

**Why PySpark instead of Pandas?**
A month is 3M+ rows. Spark processes it in parallel and writes Parquet directly, so the job never has to fit the whole month in one Python process.

**Why GCS as intermediate storage?**
Decouples processing from loading. If the BigQuery load fails, the clean Parquet is already in GCS — no need to re-run the Spark job.

**Why idempotent monthly loads?**
`load_bq.py` deletes the month before appending it, and Spark keeps only that month's trips. Re-running a failed or corrected month never duplicates data.

**Why dbt?**
Separates ingestion from business logic, documents lineage, and tests every build. Bad data fails the DAG instead of reaching the dashboard.

**Why an incremental, partitioned fact table?**
`fct_trips` merges only the latest month on `trip_key` instead of rebuilding years of history, and monthly partitions plus clustering reduce BigQuery scan costs.

**Why Docker Compose instead of managed services?**
Zero cloud spend during development. Airflow and Spark (local mode inside the Airflow container) run locally — only GCS and BigQuery are cloud services. Monthly cost: under $1.
