{{ config(materialized='table') }}

SELECT
    cancellation_nk,
    cancellation_id,
    original_transaction_id,
    reason,
    cancelled_at,
    refund_amount,
    'S3' as source,
    now() as loaded_at
FROM {{ ref('stg_cancellations') }}
