{{ config(materialized='view') }}

-- One row per trip, renamed to snake_case, with a deterministic surrogate key.
-- Spark already filtered invalid trips; the raw TLC data still contains exact
-- duplicate rows, which are removed here so trip_key is unique.

WITH source AS (
    SELECT * FROM {{ source('nyc_taxi_raw', 'yellow_taxi') }}
),

renamed AS (
    SELECT
        TO_HEX(MD5(CONCAT(
            CAST(tpep_pickup_datetime AS STRING), '|',
            CAST(tpep_dropoff_datetime AS STRING), '|',
            CAST(PULocationID AS STRING), '|',
            CAST(DOLocationID AS STRING), '|',
            CAST(passenger_count AS STRING), '|',
            CAST(trip_distance AS STRING), '|',
            CAST(payment_type AS STRING), '|',
            CAST(total_amount AS STRING)
        )))                                         AS trip_key,
        tpep_pickup_datetime                        AS pickup_datetime,
        tpep_dropoff_datetime                       AS dropoff_datetime,
        DATE(tpep_pickup_datetime)                  AS pickup_date,
        EXTRACT(HOUR FROM tpep_pickup_datetime)     AS pickup_hour,
        CAST(passenger_count AS INT64)              AS passenger_count,
        trip_distance,
        CAST(PULocationID AS INT64)                 AS pickup_location_id,
        CAST(DOLocationID AS INT64)                 AS dropoff_location_id,
        CAST(payment_type AS INT64)                 AS payment_type,
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount,
        trip_duration_minutes,
        cost_per_mile
    FROM source
    WHERE fare_amount > 0
      AND trip_distance > 0
      AND passenger_count > 0
)

SELECT *
FROM renamed
QUALIFY ROW_NUMBER() OVER (PARTITION BY trip_key ORDER BY pickup_datetime) = 1
