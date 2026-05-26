{{
    config(
        materialized='table',
        pre_hook="SET allow_experimental_join_condition = 1"
    )
}}

WITH clean_transactions AS (

    SELECT *
    FROM {{ ref('stg_transactions') }}
    WHERE is_transaction_duplicate = 0

), rates AS (
    SELECT *
    FROM {{ ref('dim_exchange_rates') }}
    ORDER BY currency, valid_from
)

SELECT
    t.transaction_nk,
    t.transaction_id,

    t.user_id as user_id,
    t.user_uuid as user_uuid,
    ifNull(u.is_test_user, 0) AS is_test_user,
    toUInt8(ifNull(u.user_uuid, '') = '' AND ifNull(t.user_uuid, '') != '') AS is_unknown_user,

    t.amount,
    t.currency as currency,
    r.rate AS exchange_rate,
    r.rate_key,
    toUInt8(ifNull(r.rate, 0) = 0) AS is_rate_missing,
    if(ifNull(r.rate, 0) = 0, NULL, t.amount / r.rate) AS amount_tgrk,

    t.transaction_type,
    t.promo_code_id as promo_code_id,
    p.code AS promo_code,
    p.max_uses AS promo_max_uses,
    p.expiry_date AS promo_expiry_date,
    t.status,
    t.created_at,

    t.is_empty_user,
    t.is_negative_amount,
    t.is_zero_amount,
    t.is_completed,
    t.is_purchase,
    
    toUInt8(t.promo_code_id IS NOT NULL) AS is_promo_used,
    toUInt8(t.promo_code_id IS NOT NULL AND ifNull(p.code, '') = '') AS is_unknown_promo,
    toUInt8(
        t.promo_code_id IS NOT NULL
        AND p.expiry_date IS NOT NULL
        AND toDate(t.created_at) > p.expiry_date
    ) AS is_promo_expired,
    'S3' as source,
    now() as loaded_at

FROM clean_transactions t

LEFT JOIN {{ ref('dim_users') }} u
    ON t.user_uuid = u.user_uuid

LEFT JOIN {{ ref('dim_promo_codes') }} p
    ON t.promo_code_id = p.promo_code_id

LEFT ASOF JOIN rates r
    ON t.currency = r.currency
   AND t.created_at >= r.valid_from

