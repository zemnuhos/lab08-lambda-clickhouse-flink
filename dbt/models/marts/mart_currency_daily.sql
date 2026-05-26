{{ config(materialized='table') }}

SELECT
    toDate(created_at) AS event_date,
    currency,

    -- Количество транзакций в валюте
    count() AS transaction_count,
    -- Количество успешных покупок в валюте
    countIf(is_purchase = 1 AND is_completed = 1) AS purchase_count,

    sum(amount) AS total_amount_original,
    sum(amount_tgrk) AS total_amount_tgrk,
    -- Сумма успешных покупок в исходной валюте
    sumIf(amount, is_purchase = 1 AND is_completed = 1) AS purchase_amount_original,
    -- Сумма успешных покупок в TGRK
    sumIf(amount_tgrk, is_purchase = 1 AND is_completed = 1) AS purchase_amount_tgrk,

    -- Средний курс валюты за день
    avgIf(exchange_rate, is_rate_missing = 0) AS avg_exchange_rate,
    -- Количество транзакций без найденного курса
    countIf(is_rate_missing = 1) AS missing_rate_count

FROM {{ ref('fact_transactions') }}

GROUP BY
    event_date,
    currency