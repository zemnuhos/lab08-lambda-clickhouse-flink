{{ config(materialized='table') }}

WITH transaction_daily AS (

    SELECT
        toDate(created_at) AS event_date,
        is_test_user,

        -- Количество транзакций
        count() AS transaction_count,
        -- Количество успешных покупок
        countIf(is_purchase = 1 AND is_completed = 1) AS purchase_count,
        -- Валовая выручка в TGRK
        sumIf(amount_tgrk, is_purchase = 1 AND is_completed = 1) AS gross_revenue_tgrk,
        -- Средний чек покупки в TGRK
        avgIf(amount_tgrk, is_purchase = 1 AND is_completed = 1) AS avg_purchase_amount_tgrk
    FROM {{ ref('fact_transactions') }}
    GROUP BY
        event_date,
        is_test_user

),

cancellation_daily AS (

    SELECT
        toDate(cancelled_at) AS event_date,
        ifNull(matched_is_test_user, 0) AS is_test_user,
        -- Сумма сопоставленных отмен
        sumIf(abs(matched_refund_amount_tgrk), match_status = 'matched') AS matched_refunds_tgrk,
        -- Количество сопоставленных отмен
        countIf(match_status = 'matched') AS matched_cancellation_count
    FROM {{ ref('fact_cancellation_matches') }}
    WHERE cancelled_at IS NOT NULL
    GROUP BY
        event_date,
        is_test_user

)

SELECT
    t.event_date,
    t.is_test_user,
    t.transaction_count,
    t.purchase_count,
    t.gross_revenue_tgrk,
    -- Сумма сопоставленных отмен
    ifNull(c.matched_refunds_tgrk, 0) AS matched_refunds_tgrk,
    -- Чистая выручка после отмен
    t.gross_revenue_tgrk - ifNull(c.matched_refunds_tgrk, 0) AS net_revenue_tgrk,
    t.avg_purchase_amount_tgrk,
    -- Количество сопоставленных отмен
    ifNull(c.matched_cancellation_count, 0) AS matched_cancellation_count
FROM transaction_daily t
LEFT JOIN cancellation_daily c
    ON t.event_date = c.event_date
   AND t.is_test_user = c.is_test_user

SETTINGS join_use_nulls = 1