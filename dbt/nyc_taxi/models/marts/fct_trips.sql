{{ config(
    materialized='table',
    cluster_by=['pickup_location_id', 'payment_type']
) }}

WITH trips AS (
    SELECT * FROM {{ ref('stg_yellow_taxi') }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY pickup_datetime)    AS trip_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    pickup_location_id,
    dropoff_location_id,
    payment_type,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    trip_duration_minutes,
    cost_per_mile,
    CASE payment_type
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        ELSE 'Unknown'
    END                                             AS payment_type_desc
FROM trips
