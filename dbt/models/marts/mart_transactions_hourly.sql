{{ config(materialized='table') }}

SELECT
    toStartOfHour(created_at) AS created_hour,
    transaction_type,
    status,
    currency,
    is_test_user,

    -- Количество транзакций
    count() AS transaction_count,
    -- Количество транзакций типа purchase
    countIf(is_purchase = 1) AS purchase_type_count,
    -- Количество успешных транзакций
    countIf(is_completed = 1) AS completed_count,
    -- Количество успешных покупок
    countIf(is_purchase = 1 AND is_completed = 1) AS completed_purchase_count,

    -- Сумма транзакций в исходной валюте
    sum(amount) AS amount_original,
    -- Сумма транзакций в базовой валюте TGRK
    sum(amount_tgrk) AS amount_tgrk,

    -- Количество транзакций с отрицательной суммой
    countIf(is_negative_amount = 1) AS negative_amount_count,
    -- Количество транзакций с нулевой суммой
    countIf(is_zero_amount = 1) AS zero_amount_count,
    -- Количество транзакций без найденного курса валют
    countIf(is_rate_missing = 1) AS missing_rate_count
FROM {{ ref('fact_transactions') }}
GROUP BY
    created_hour,
    transaction_type,
    status,
    currency,
    is_test_user