{{ config(materialized='table') }}

SELECT
    transaction_nk,
    transaction_id,
    user_id,
    user_uuid,
    amount,
    currency,
    transaction_type,
    promo_code_id,
    status,
    created_at,
    transaction_dup_cnt,
    is_transaction_duplicate,
    'S3' as source,
    now() as loaded_at
FROM {{ ref('stg_transactions') }}
WHERE is_transaction_duplicate = 1
order by transaction_nk
