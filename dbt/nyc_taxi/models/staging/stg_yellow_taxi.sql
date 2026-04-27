{{ config(materialized='view') }}

SELECT
    tpep_pickup_datetime                        AS pickup_datetime,
    tpep_dropoff_datetime                       AS dropoff_datetime,
    passenger_count,
    trip_distance,
    PULocationID                                AS pickup_location_id,
    DOLocationID                                AS dropoff_location_id,
    payment_type,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    trip_duration_minutes,
    cost_per_mile
FROM {{ source('nyc_taxi_raw', 'yellow_taxi') }}
WHERE fare_amount > 0
  AND trip_distance > 0
  AND passenger_count > 0
