{{ config(materialized='table') }}

WITH purchases AS (

    SELECT
        user_uuid,
        is_test_user,
        toDate(created_at) AS activity_date,
        transaction_nk,
        amount_tgrk
    FROM {{ ref('fact_transactions') }}
    WHERE is_purchase = 1
      AND is_completed = 1
      AND user_uuid IS NOT NULL

),

cohorts AS (

    SELECT
        user_uuid,
        min(activity_date) AS cohort_date
    FROM purchases
    GROUP BY user_uuid

)

SELECT
    c.cohort_date,
    p.activity_date,
    -- Количество дней с момента первой покупки пользователя
    dateDiff('day', c.cohort_date, p.activity_date) AS days_since_cohort,
    p.is_test_user,

    -- Количество уникальных пользователей в когорте
    uniqExact(p.user_uuid) AS users_count,
    -- Количество успешных покупок
    count() AS purchase_count,
    -- Выручка когорты в TGRK
    sum(p.amount_tgrk) AS revenue_tgrk
FROM purchases p
INNER JOIN cohorts c
    ON p.user_uuid = c.user_uuid
GROUP BY
    c.cohort_date,
    p.activity_date,
    days_since_cohort,
    p.is_test_user