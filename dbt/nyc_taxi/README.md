# nyc_taxi dbt project

Star schema on top of `nyc_taxi_raw.yellow_taxi`:

| Model | Type | Grain / purpose |
|---|---|---|
| `stg_yellow_taxi` | view | Renamed, deduplicated trips with `trip_key` |
| `fct_trips` | incremental table | One row per trip, partitioned by month |
| `dim_location` | table | TLC taxi zones (seed `taxi_zone_lookup`) |
| `dim_payment_type` | table | Payment codes (seed `payment_types`) |
| `dim_date` | table | Calendar days |

Run everything (seeds, models, tests) with:

```bash
dbt build --profiles-dir /opt/airflow/dbt --target dev
```
