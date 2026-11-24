-- One row per calendar day covering every pickup date in the trips data
WITH bounds AS (
    SELECT MIN(pickup_date) AS min_date, MAX(pickup_date) AS max_date
    FROM {{ ref('stg_yellow_taxi') }}
)

SELECT
    d                                           AS date_day,
    EXTRACT(YEAR FROM d)                        AS year,
    EXTRACT(QUARTER FROM d)                     AS quarter,
    EXTRACT(MONTH FROM d)                       AS month,
    FORMAT_DATE('%B', d)                        AS month_name,
    EXTRACT(DAY FROM d)                         AS day_of_month,
    EXTRACT(DAYOFWEEK FROM d)                   AS day_of_week,      -- 1 = Sunday
    FORMAT_DATE('%A', d)                        AS day_name,
    EXTRACT(DAYOFWEEK FROM d) IN (1, 7)         AS is_weekend
FROM bounds, UNNEST(GENERATE_DATE_ARRAY(min_date, max_date)) AS d
