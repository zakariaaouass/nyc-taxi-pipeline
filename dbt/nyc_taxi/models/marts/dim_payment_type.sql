-- Payment type codes from the TLC data dictionary (seeds/payment_types.csv)
SELECT
    payment_type,
    payment_type_desc
FROM {{ ref('payment_types') }}
