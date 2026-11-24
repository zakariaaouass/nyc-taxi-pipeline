{{ config(
    materialized='incremental',
    unique_key='trip_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    partition_by={'field': 'pickup_datetime', 'data_type': 'timestamp', 'granularity': 'month'},
    cluster_by=['pickup_location_id', 'payment_type']
) }}

-- Grain: one row per taxi trip.
-- Incremental: each run re-merges the latest loaded month (and anything newer),
-- so a new month is appended and a rerun of the same month does not create duplicates.

SELECT
    trip_key,
    pickup_datetime,
    dropoff_datetime,
    pickup_date,                       -- FK -> dim_date.date_day
    pickup_hour,
    pickup_location_id,                -- FK -> dim_location.location_id
    dropoff_location_id,               -- FK -> dim_location.location_id
    payment_type,                      -- FK -> dim_payment_type.payment_type
    passenger_count,
    trip_distance,
    trip_duration_minutes,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    cost_per_mile
FROM {{ ref('stg_yellow_taxi') }}

{% if is_incremental() %}
WHERE pickup_datetime >= (
    SELECT TIMESTAMP_TRUNC(MAX(pickup_datetime), MONTH) FROM {{ this }}
)
{% endif %}
