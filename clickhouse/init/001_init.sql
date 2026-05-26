CREATE DATABASE IF NOT EXISTS analytics;

--------------------------------------------------
-- LOAD LOG
--------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics.load_log
(
    source_type String,
    source_file String,
    file_size UInt64,
    loaded_rows UInt64,
    status String,
    error_message String,
    started_at DateTime,
    finished_at DateTime
)
ENGINE = MergeTree
ORDER BY (source_type, source_file, started_at);

--------------------------------------------------
-- RAW TRANSACTIONS
--------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics.raw_transactions
(
    transaction_id UInt64,

    user_id Nullable(UInt64),

    user_uuid String,

    amount Float64,

    currency LowCardinality(String),

    transaction_type LowCardinality(String),

    promo_code_id Nullable(UInt64),

    status LowCardinality(String),

    created_at UInt64,

    source_file String,

    loaded_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY transaction_id;

--------------------------------------------------
-- RAW CANCELLATIONS
--------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics.raw_cancellations
(
    cancellation_id UInt64,

    original_transaction_id UInt64,

    reason LowCardinality(String),

    cancelled_at String,

    refund_amount Float64,

    source_file String,

    loaded_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (
    cancellation_id,
    original_transaction_id
);

--------------------------------------------------
-- RAW EXCHANGE RATES
--------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics.raw_exchange_rates
(
    update_id UInt64,

    timestamp String,

    rate_tgrk_punk Float64,

    rate_tgrk_rub Float64,

    source_file String,

    loaded_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (timestamp,update_id);

--------------------------------------------------
-- RAW USERS
--------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics.raw_users
(
    user_id UInt64,

    user_uuid String,

    is_test_user Bool,

    source_file String,

    loaded_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY user_id;

--------------------------------------------------
-- RAW TEST USERS
--------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics.raw_test_users
(
    test_user_uuid String,

    source_file String,

    loaded_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY test_user_uuid;

--------------------------------------------------
-- RAW PROMO CODES
--------------------------------------------------

CREATE TABLE IF NOT EXISTS analytics.raw_promo_codes
(
    promo_code_id UInt64,

    code String,

    max_uses UInt64,

    expiry_date String,

    source_file String,

    loaded_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY promo_code_id;

