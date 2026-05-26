{{ config(materialized='table') }}

WITH candidates AS (

    SELECT
        c.cancellation_nk,
        c.cancellation_id,
        c.original_transaction_id,
        c.reason,
        c.cancelled_at,
        c.refund_amount,
        if(t.transaction_id != 0, 1, 0) AS is_matched,

        if(is_matched = 1, toNullable(t.transaction_nk), NULL) AS transaction_nk,
        if(is_matched = 1, toNullable(t.created_at), NULL) AS transaction_created_at,
        if(is_matched = 1, toNullable(t.amount), NULL) AS transaction_amount,
        if(is_matched = 1, toNullable(t.amount_tgrk), NULL) AS transaction_amount_tgrk,
        if(is_matched = 1, toNullable(t.currency), NULL) AS transaction_currency,
        if(is_matched = 1, toNullable(t.is_test_user), NULL) AS is_test_user

    FROM {{ ref('fact_cancellations') }} c

    LEFT JOIN {{ ref('fact_transactions') }} t
        ON c.original_transaction_id = t.transaction_id
       AND abs(refund_amount) = abs(t.amount)
       AND toDate(t.created_at) = 
            addDays(toDate(c.cancelled_at), -1)
),

aggregated AS (

    SELECT
        cancellation_nk,
        any(cancellation_id) AS cancellation_id,
        any(original_transaction_id) AS original_transaction_id,
        any(reason) AS reason,
        any(cancelled_at) AS cancelled_at,
        any(refund_amount) AS refund_amount,
        
        countIf(transaction_nk IS NOT NULL) AS candidate_count,
        anyIf(transaction_nk, transaction_nk IS NOT NULL) AS any_transaction_nk,
        anyIf(transaction_created_at, transaction_nk IS NOT NULL) AS any_transaction_created_at,
        anyIf(transaction_amount_tgrk, transaction_nk IS NOT NULL) AS any_transaction_amount_tgrk,
        anyIf(transaction_currency, transaction_nk IS NOT NULL) AS any_transaction_currency,
        anyIf(is_test_user, transaction_nk IS NOT NULL) AS any_is_test_user

    FROM candidates
    GROUP BY cancellation_nk

)

SELECT
    cancellation_nk,
    cancellation_id,
    original_transaction_id,
    reason,
    cancelled_at,
    refund_amount,
    candidate_count,

    multiIf(
        candidate_count = 0, 'unmatched',
        candidate_count = 1, 'matched',
        'ambiguous'
    ) AS match_status,

    if(candidate_count = 1, any_transaction_nk, NULL) AS matched_transaction_nk,
    if(candidate_count = 1, any_transaction_created_at, NULL) AS matched_transaction_created_at,
    if(candidate_count = 1, any_transaction_amount_tgrk, NULL) AS matched_refund_amount_tgrk,
    if(candidate_count = 1, any_transaction_currency, NULL) AS matched_currency,
    if(candidate_count = 1, any_is_test_user, NULL) AS matched_is_test_user,
    if(
        candidate_count = 1,
        dateDiff('hour', any_transaction_created_at, cancelled_at),
        NULL
    ) AS hours_to_cancel,
    'S3' as source,
    now() as loaded_at

FROM aggregated

SETTINGS join_use_nulls = 1