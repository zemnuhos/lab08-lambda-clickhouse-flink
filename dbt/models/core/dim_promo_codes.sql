{{ config(materialized='table') }}

SELECT
    promo_code_id,
    any(code) AS code,
    any(max_uses) AS max_uses,
    any(expiry_date) AS expiry_date,
    'S3' as source,
    now() as loaded_at
FROM {{ ref('stg_promo_codes') }}
GROUP BY promo_code_id
