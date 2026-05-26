--------------------------------------------------
-- REAL-TIME STREAM TABLES
--------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics.rt_transactions
(
    event_id String,
    transaction_id UInt64,
    user_id Nullable(UInt64),
    user_uuid Nullable(String),
    amount Float64,
    currency LowCardinality(String),
    transaction_type LowCardinality(String),
    promo_code_id Nullable(UInt64),
    status LowCardinality(String),
    created_at DateTime64(3, 'UTC'),
    source LowCardinality(String) DEFAULT 'kafka',
    loaded_at DateTime64(3, 'UTC'),
    published_at Nullable(DateTime64(3, 'UTC')),
    transaction_dup_rn UInt32 DEFAULT 1,
    transaction_dup_cnt UInt32 DEFAULT 1,
    is_transaction_duplicate UInt8 DEFAULT 0,
    is_empty_user UInt8,
    is_negative_amount UInt8,
    is_zero_amount UInt8,
    is_completed UInt8,
    is_purchase UInt8
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (created_at, transaction_id, event_id);

CREATE TABLE IF NOT EXISTS analytics.rt_cancellations
(
    event_id String,
    original_transaction_id UInt64,
    reason LowCardinality(String),
    cancelled_at DateTime64(3, 'UTC'),
    refund_amount Float64,
    source LowCardinality(String) DEFAULT 'kafka',
    loaded_at DateTime64(3, 'UTC'),
    published_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (cancelled_at, original_transaction_id, event_id);

CREATE TABLE IF NOT EXISTS analytics.rt_exchange_rates
(
    event_id String,
    update_id UInt64,
    ts DateTime64(3, 'UTC'),
    currency LowCardinality(String),
    rate Float64,
    source LowCardinality(String) DEFAULT 'kafka',
    loaded_at DateTime64(3, 'UTC'),
    published_at Nullable(DateTime64(3, 'UTC'))
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY (currency, ts, update_id, event_id);

--------------------------------------------------
-- REAL-TIME MARTS / SUPERSET VIEWS
--------------------------------------------------

DROP VIEW IF EXISTS analytics.mart_rt_current_hour_metrics;
DROP VIEW IF EXISTS analytics.mart_rt_events_per_minute;
DROP VIEW IF EXISTS analytics.mart_rt_data_quality_last_60m;

CREATE VIEW analytics.mart_rt_current_hour_metrics AS
SELECT
    toStartOfHour(now64(3, 'UTC')) AS current_hour,
    now64(3, 'UTC') - INTERVAL 60 MINUTE AS window_started_at,
    now64(3, 'UTC') AS calculated_at,
    toString('kafka') AS source,
    toUInt64(count()) AS transaction_count,
    toUInt64(countIf(is_purchase = 1)) AS purchase_type_count,
    toUInt64(countIf(is_completed = 1)) AS completed_count,
    toUInt64(countIf(is_purchase = 1 AND is_completed = 1)) AS completed_purchase_count,
    currency,
    toFloat64(sum(amount)) AS amount_original,
    toFloat64(sumIf(amount, is_purchase = 1 AND is_completed = 1)) AS completed_purchase_amount_original,
    toUInt64(countIf(is_negative_amount = 1)) AS negative_amount_count,
    toUInt64(countIf(is_zero_amount = 1)) AS zero_amount_count,
    toUInt64(countIf(is_empty_user = 1)) AS empty_user_count,
    if(
        count() = 0,
        toDateTime64('1970-01-01 00:00:00', 3, 'UTC'),
        max(loaded_at)
    ) AS last_loaded_at
FROM analytics.rt_transactions
WHERE loaded_at >= now64(3, 'UTC') - INTERVAL 60 MINUTE
    group by currency;

CREATE VIEW analytics.mart_rt_events_per_minute AS
SELECT
    event_minute,
    source,
    event_type,
    toUInt64(count()) AS event_count
FROM
(
    SELECT
        toStartOfMinute(loaded_at) AS event_minute,
        toString(source) AS source,
        toString('transaction') AS event_type
    FROM analytics.rt_transactions
    WHERE loaded_at >= now64(3, 'UTC') - INTERVAL 60 MINUTE

    UNION ALL

    SELECT
        toStartOfMinute(loaded_at) AS event_minute,
        toString(source) AS source,
        toString('cancellation') AS event_type
    FROM analytics.rt_cancellations
    WHERE loaded_at >= now64(3, 'UTC') - INTERVAL 60 MINUTE

    UNION ALL

    SELECT
        toStartOfMinute(loaded_at) AS event_minute,
        toString(source) AS source,
        toString('exchange_rate') AS event_type
    FROM analytics.rt_exchange_rates
    WHERE loaded_at >= now64(3, 'UTC') - INTERVAL 60 MINUTE
)
GROUP BY
    event_minute,
    source,
    event_type;

CREATE VIEW analytics.mart_rt_data_quality_last_60m AS
SELECT
    now64(3, 'UTC') - INTERVAL 60 MINUTE AS window_started_at,
    now64(3, 'UTC') AS calculated_at,
    toString('kafka') AS source,
    toUInt64(count()) AS transaction_count,
    toUInt64(countIf(is_empty_user = 1)) AS empty_user_count,
    toUInt64(countIf(is_negative_amount = 1)) AS negative_amount_count,
    toUInt64(countIf(is_zero_amount = 1)) AS zero_amount_count,
    toUInt64(countIf(is_transaction_duplicate = 1)) AS duplicate_transaction_count,
    toFloat64(if(count() = 0, 0, countIf(is_empty_user = 1) / count())) AS empty_user_share,
    toFloat64(if(count() = 0, 0, countIf(is_negative_amount = 1) / count())) AS negative_amount_share,
    toFloat64(if(count() = 0, 0, countIf(is_zero_amount = 1) / count())) AS zero_amount_share
FROM analytics.rt_transactions
WHERE loaded_at >= now64(3, 'UTC') - INTERVAL 60 MINUTE;

