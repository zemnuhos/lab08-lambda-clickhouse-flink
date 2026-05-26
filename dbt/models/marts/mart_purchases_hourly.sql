{{ config(materialized='table') }}

SELECT
    toStartOfHour(created_at) AS created_hour,
    currency,
    is_test_user,

    -- Количество успешных покупок
    count() AS purchase_count,
    -- Сумма покупок в исходной валюте
    sum(amount) AS purchase_amount_original,
    -- Сумма покупок в базовой валюте TGRK
    sum(amount_tgrk) AS purchase_amount_tgrk,
    -- Средний чек покупки в TGRK
    avg(amount_tgrk) AS avg_purchase_amount_tgrk,

    -- Количество покупок с отрицательной суммой
    countIf(is_negative_amount = 1) AS negative_purchase_count,
    -- Количество покупок с нулевой суммой
    countIf(is_zero_amount = 1) AS zero_purchase_count,
    -- Количество покупок без найденного курса валют
    countIf(is_rate_missing = 1) AS missing_rate_count
FROM {{ ref('fact_transactions') }}
WHERE is_purchase = 1
  AND is_completed = 1
GROUP BY
    created_hour,
    currency,
    is_test_user