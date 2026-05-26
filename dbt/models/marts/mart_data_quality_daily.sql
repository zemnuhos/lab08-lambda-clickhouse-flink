{{ config(materialized='table') }}

WITH tx_quality AS (

    SELECT
        toDate(created_at) AS event_date,

        -- Количество строк транзакций в staging
        count() AS raw_transaction_rows,
        -- Количество чистых транзакций без дублей
        countIf(is_transaction_duplicate = 0) AS clean_transaction_rows,
        -- Количество строк с дублирующимися transaction_id
        countIf(is_transaction_duplicate = 1) AS duplicate_transaction_rows,
        -- Количество групп дублей transaction_id
        uniqExactIf(transaction_nk, is_transaction_duplicate = 1) AS duplicate_transaction_groups,

        -- Количество транзакций без пользователя
        countIf(is_empty_user = 1) AS empty_user_rows,
        -- Количество транзакций с отрицательной суммой
        countIf(is_negative_amount = 1) AS negative_amount_rows,
        -- Количество транзакций с нулевой суммой
        countIf(is_zero_amount = 1) AS zero_amount_rows,
        -- Сумма транзакций, исключенных как дубли
        sumIf(amount, is_transaction_duplicate = 1) AS duplicate_amount_original
    FROM {{ ref('stg_transactions') }}
    GROUP BY event_date

),

duplicate_quality AS (

    SELECT
        toDate(created_at) AS event_date,
        -- Количество строк, вынесенных в dq_transactions_duplicate
        count() AS quarantined_duplicate_rows,
        -- Количество групп дублей в dq_transactions_duplicate
        uniqExact(transaction_nk) AS quarantined_duplicate_groups
    FROM {{ ref('dq_transactions_duplicate') }}
    GROUP BY event_date

),

fact_quality AS (

    SELECT
        toDate(created_at) AS event_date,
        -- Количество транзакций с неизвестным пользователем
        countIf(is_unknown_user = 1) AS unknown_user_rows,
        -- Количество транзакций с неизвестным промокодом
        countIf(is_unknown_promo = 1) AS unknown_promo_rows,
        -- Количество транзакций с просроченным промокодом
        countIf(is_promo_expired = 1) AS expired_promo_rows,
        -- Количество транзакций без найденного курса
        countIf(is_rate_missing = 1) AS missing_rate_rows
    FROM {{ ref('fact_transactions') }}
    GROUP BY event_date

),

cancellation_quality AS (

    SELECT
        toDate(cancelled_at) AS event_date,
        -- Количество успешно сопоставленных отмен
        countIf(match_status = 'matched') AS matched_cancellation_rows,
        -- Количество неоднозначных отмен
        countIf(match_status = 'ambiguous') AS ambiguous_cancellation_rows,
        -- Количество несопоставленных отмен
        countIf(match_status = 'unmatched') AS unmatched_cancellation_rows
    FROM {{ ref('fact_cancellation_matches') }}
    WHERE cancelled_at IS NOT NULL
    GROUP BY event_date

),

dates AS (

    SELECT event_date FROM tx_quality
    UNION DISTINCT
    SELECT event_date FROM duplicate_quality
    UNION DISTINCT
    SELECT event_date FROM fact_quality
    UNION DISTINCT
    SELECT event_date FROM cancellation_quality

)

SELECT
    d.event_date as event_date,

    -- Количество строк транзакций в staging
    ifNull(t.raw_transaction_rows, 0) AS raw_transaction_rows,
    -- Количество чистых транзакций без дублей
    ifNull(t.clean_transaction_rows, 0) AS clean_transaction_rows,
    -- Количество строк с дублирующимися transaction_id
    ifNull(t.duplicate_transaction_rows, 0) AS duplicate_transaction_rows,
    -- Количество групп дублей transaction_id
    ifNull(t.duplicate_transaction_groups, 0) AS duplicate_transaction_groups,

    -- Количество строк, вынесенных в dq_transactions_duplicate
    ifNull(q.quarantined_duplicate_rows, 0) AS quarantined_duplicate_rows,
    -- Количество групп дублей в dq_transactions_duplicate
    ifNull(q.quarantined_duplicate_groups, 0) AS quarantined_duplicate_groups,

    -- Количество транзакций без пользователя
    ifNull(t.empty_user_rows, 0) AS empty_user_rows,
    -- Количество транзакций с отрицательной суммой
    ifNull(t.negative_amount_rows, 0) AS negative_amount_rows,
    -- Количество транзакций с нулевой суммой
    ifNull(t.zero_amount_rows, 0) AS zero_amount_rows,
    -- Сумма транзакций, исключенных как дубли
    ifNull(t.duplicate_amount_original, 0) AS duplicate_amount_original,

    -- Количество транзакций с неизвестным пользователем
    ifNull(f.unknown_user_rows, 0) AS unknown_user_rows,
    -- Количество транзакций с неизвестным промокодом
    ifNull(f.unknown_promo_rows, 0) AS unknown_promo_rows,
    -- Количество транзакций с просроченным промокодом
    ifNull(f.expired_promo_rows, 0) AS expired_promo_rows,
    -- Количество транзакций без найденного курса
    ifNull(f.missing_rate_rows, 0) AS missing_rate_rows,

    -- Количество успешно сопоставленных отмен
    ifNull(c.matched_cancellation_rows, 0) AS matched_cancellation_rows,
    -- Количество неоднозначных отмен
    ifNull(c.ambiguous_cancellation_rows, 0) AS ambiguous_cancellation_rows,
    -- Количество несопоставленных отмен
    ifNull(c.unmatched_cancellation_rows, 0) AS unmatched_cancellation_rows
FROM dates d
LEFT JOIN tx_quality t
    ON d.event_date = t.event_date
LEFT JOIN duplicate_quality q
    ON d.event_date = q.event_date
LEFT JOIN fact_quality f
    ON d.event_date = f.event_date
LEFT JOIN cancellation_quality c
    ON d.event_date = c.event_date

SETTINGS join_use_nulls = 1