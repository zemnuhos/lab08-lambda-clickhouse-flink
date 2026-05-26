{{ config(materialized='table') }}

WITH rates AS (

    SELECT
        update_id,
        currency,
        ts AS valid_from,
        rate,
        'S3' as source,
        now() as loaded_at
    FROM {{ ref('stg_exchange_rates') }}

),

numbered AS (

    SELECT
        *,
        row_number() OVER (
            PARTITION BY currency
            ORDER BY valid_from
        ) AS rn,

        count() OVER (
            PARTITION BY currency
        ) AS cnt
    FROM rates

),

with_intervals AS (

    SELECT
        n1.update_id,
        n1.currency,
        n1.valid_from,

        if(
            n1.rn = n1.cnt,
            toDateTime('2100-01-01 00:00:00'),
            n2.valid_from
        ) AS valid_to,

        n1.rate,
        n1.source,
        n1.loaded_at
    FROM numbered n1
    LEFT JOIN numbered n2
        ON n1.currency = n2.currency
       AND n1.rn + 1 = n2.rn

)

SELECT
    cityHash64(currency, valid_from) AS rate_key,
    update_id,
    currency,
    valid_from,
    valid_to,
    rate,
    valid_to = toDateTime('2100-01-01 00:00:00') AS is_current,
    source,
    loaded_at
FROM with_intervals
