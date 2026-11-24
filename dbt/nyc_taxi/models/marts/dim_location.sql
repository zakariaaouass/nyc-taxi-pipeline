-- Taxi zones from the official TLC lookup (seeds/taxi_zone_lookup.csv)
SELECT
    LocationID      AS location_id,
    Borough         AS borough,
    Zone            AS zone,
    service_zone
FROM {{ ref('taxi_zone_lookup') }}
