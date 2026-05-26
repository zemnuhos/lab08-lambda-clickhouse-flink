{{ config(materialized='view') }}

SELECT

    promo_code_id,

    code,

    max_uses,

    toDateOrNull(expiry_date)
        AS expiry_date,

    source_file,

    loaded_at

FROM {{ source('analytics', 'raw_promo_codes') }}
