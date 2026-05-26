{{ config(materialized='table') }}

WITH promo_usage AS (

    SELECT
        p.promo_code_id,
        p.code,
        p.max_uses,
        p.expiry_date,
        t.is_test_user,
        -- Количество всех использований промокода
        countIf(t.transaction_nk IS NOT NULL) AS uses_total,
        -- Количество успешных покупок с промокодом
        countIf(t.is_purchase = 1 AND t.is_completed = 1) AS uses_completed_purchase,
        -- Количество использований просроченного промокода
        countIf(t.is_promo_expired = 1) AS uses_expired,
        -- Выручка по успешным покупкам с промокодом в TGRK
        sumIf(t.amount_tgrk, t.is_purchase = 1 AND t.is_completed = 1) AS revenue_tgrk
    FROM {{ ref('dim_promo_codes') }} p
    LEFT JOIN {{ ref('fact_transactions') }} t
        ON p.promo_code_id = t.promo_code_id
    GROUP BY
        p.promo_code_id,
        p.code,
        p.max_uses,
        p.expiry_date,
		t.is_test_user

)

SELECT
    promo_code_id,
    code,
    max_uses,
    uses_total - uses_expired as real_uses,
    is_test_user,
    expiry_date,

    uses_total,
    uses_completed_purchase,
    uses_expired,

    -- Процент использования лимита промокода
    if(max_uses = 0 OR max_uses IS NULL, NULL, uses_completed_purchase / greatest(max_uses,real_uses)) AS usage_pct,
    -- Превышен ли лимит использования промокода
    toUInt8(max_uses IS NOT NULL AND uses_completed_purchase > max_uses) AS is_limit_exceeded,
    revenue_tgrk
FROM promo_usage