{{ config(materialized='table') }}

WITH transaction_daily AS (

    SELECT
        toDate(created_at) AS revenue_date,
        is_test_user,

        -- Валовая выручка в TGRK до вычета отмен
        sumIf(amount_tgrk, is_purchase = 1 AND is_completed = 1) AS gross_revenue_tgrk,
        -- Количество успешных покупок
        countIf(is_purchase = 1 AND is_completed = 1) AS purchase_count,
        -- Количество транзакций
        count() AS transaction_count,
        -- Количество транзакций без найденного курса
        countIf(is_rate_missing = 1) AS missing_rate_count
    FROM {{ ref('fact_transactions') }}
    GROUP BY
        revenue_date,
        is_test_user

),

cancellation_daily AS (

    SELECT
        toDate(cancelled_at) AS revenue_date,
        ifNull(matched_is_test_user, 0) AS is_test_user,

        -- Сумма отмен, однозначно сопоставленных с транзакциями
        sumIf(abs(matched_refund_amount_tgrk), match_status = 'matched') AS matched_refunds_tgrk,
        -- Количество однозначно сопоставленных отмен
        countIf(match_status = 'matched') AS matched_cancellation_count,
        -- Количество неоднозначных отмен
        countIf(match_status = 'ambiguous') AS ambiguous_cancellation_count,
        -- Количество несопоставленных отмен
        countIf(match_status = 'unmatched') AS unmatched_cancellation_count,
        sumIf(abs(refund_amount), match_status = 'ambiguous') AS ambiguous_refund_amount_original,
        sumIf(abs(refund_amount), match_status = 'unmatched') AS unmatched_refund_amount_original
    FROM {{ ref('fact_cancellation_matches') }}
    WHERE cancelled_at IS NOT NULL
    GROUP BY
        revenue_date,
        is_test_user

),

dates AS (

    SELECT revenue_date, is_test_user FROM transaction_daily
    UNION DISTINCT
    SELECT revenue_date, is_test_user FROM cancellation_daily

)

SELECT
    d.revenue_date as revenue_date,
    d.is_test_user as is_test_user,

    -- Валовая выручка в TGRK до вычета отмен
    ifNull(t.gross_revenue_tgrk, 0) AS gross_revenue_tgrk,
    -- Сумма отмен, однозначно сопоставленных с транзакциями
    ifNull(c.matched_refunds_tgrk, 0) AS matched_refunds_tgrk,
    -- Чистая выручка после вычета сопоставленных отмен
    ifNull(t.gross_revenue_tgrk, 0) - ifNull(c.matched_refunds_tgrk, 0) AS net_revenue_tgrk,

    -- Количество успешных покупок
    ifNull(t.purchase_count, 0) AS purchase_count,
    -- Количество транзакций
    ifNull(t.transaction_count, 0) AS transaction_count,
    -- Количество транзакций без найденного курса
    ifNull(t.missing_rate_count, 0) AS missing_rate_count,

    -- Количество однозначно сопоставленных отмен
    ifNull(c.matched_cancellation_count, 0) AS matched_cancellation_count,
    -- Количество неоднозначных отмен
    ifNull(c.ambiguous_cancellation_count, 0) AS ambiguous_cancellation_count,
    -- Количество несопоставленных отмен
    ifNull(c.unmatched_cancellation_count, 0) AS unmatched_cancellation_count,
    ifNull(c.ambiguous_refund_amount_original, 0) AS ambiguous_refund_amount_original,
    ifNull(c.unmatched_refund_amount_original, 0) AS unmatched_refund_amount_original
FROM dates d
LEFT JOIN transaction_daily t
    ON d.revenue_date = t.revenue_date
   AND d.is_test_user = t.is_test_user
LEFT JOIN cancellation_daily c
    ON d.revenue_date = c.revenue_date
   AND d.is_test_user = c.is_test_user

SETTINGS join_use_nulls = 1