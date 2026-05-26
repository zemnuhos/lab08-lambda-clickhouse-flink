{{ config(materialized='table') }}

WITH cancellations AS (

    SELECT
        toDate(cancelled_at) AS cancelled_date,
        reason,
        match_status,

        -- Количество отмен
        count() AS cancellation_count,
        -- Сумма отмен в исходной валюте/значении
        sum(abs(refund_amount)) AS refund_amount_original,
        -- Сумма сопоставленных отмен в TGRK
        sumIf(abs(matched_refund_amount_tgrk), match_status = 'matched') AS matched_refund_amount_tgrk,
        -- Среднее время от транзакции до отмены в часах
        avgIf(hours_to_cancel, match_status = 'matched') AS avg_hours_to_cancel
    FROM {{ ref('fact_cancellation_matches') }}
    WHERE cancelled_at IS NOT NULL
    GROUP BY
        cancelled_date,
        reason,
        match_status

),

purchases AS (

    SELECT
        toDate(created_at) AS purchase_date,
        -- Количество успешных покупок
        countIf(is_purchase = 1 AND is_completed = 1) AS purchase_count
    FROM {{ ref('fact_transactions') }}
    GROUP BY purchase_date

)

SELECT
    c.cancelled_date,
    c.reason,
    c.match_status,
    c.cancellation_count,
    c.refund_amount_original,
    c.matched_refund_amount_tgrk,
    c.avg_hours_to_cancel,
    -- Количество успешных покупок
    ifNull(p.purchase_count, 0) AS purchase_count,
    -- Доля отмен относительно успешных покупок
    if(p.purchase_count = 0 OR p.purchase_count IS NULL, NULL, c.cancellation_count / p.purchase_count) AS cancellation_rate
FROM cancellations c
LEFT JOIN purchases p
    ON c.cancelled_date = p.purchase_date

SETTINGS join_use_nulls = 1