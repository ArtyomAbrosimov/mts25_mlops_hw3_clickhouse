CREATE TABLE IF NOT EXISTS transactions_kafka_raw (
    raw_message String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'clickhouse_transactions_group_v2',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 2;

CREATE TABLE IF NOT EXISTS transactions (
    transaction_time DateTime,
    merch LowCardinality(String),
    cat_id LowCardinality(String),
    amount Decimal64(2),
    name_1 String,
    name_2 String,
    gender LowCardinality(String),
    street String,
    one_city LowCardinality(String),
    us_state LowCardinality(String),
    post_code String,
    lat Float64,
    lon Float64,
    population_city UInt64,
    jobs LowCardinality(String),
    merchant_lat Float64,
    merchant_lon Float64,
    target UInt8,
    PROJECTION proj_max_transaction_by_state_category (
        SELECT
            us_state,
            cat_id,
            MAX(amount)
        GROUP BY us_state, cat_id
    )

) ENGINE = MergeTree()
PARTITION BY toYYYYMM(transaction_time)
ORDER BY (us_state, cat_id, transaction_time)
SETTINGS
    index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS transactions_mv TO transactions AS
SELECT
    parseDateTimeBestEffort(JSONExtractString(raw_message, 'transaction_time')) as transaction_time,
    toLowCardinality(JSONExtractString(raw_message, 'merch')) as merch,
    toLowCardinality(JSONExtractString(raw_message, 'cat_id')) as cat_id,
    toDecimal64(JSONExtractFloat(raw_message, 'amount'), 2) as amount,
    JSONExtractString(raw_message, 'name_1') as name_1,
    JSONExtractString(raw_message, 'name_2') as name_2,
    toLowCardinality(JSONExtractString(raw_message, 'gender')) as gender,
    JSONExtractString(raw_message, 'street') as street,
    toLowCardinality(JSONExtractString(raw_message, 'one_city')) as one_city,
    toLowCardinality(JSONExtractString(raw_message, 'us_state')) as us_state,
    JSONExtractString(raw_message, 'post_code') as post_code,
    JSONExtractFloat(raw_message, 'lat') as lat,
    JSONExtractFloat(raw_message, 'lon') as lon,
    JSONExtractUInt(raw_message, 'population_city') as population_city,
    toLowCardinality(JSONExtractString(raw_message, 'jobs')) as jobs,
    JSONExtractFloat(raw_message, 'merchant_lat') as merchant_lat,
    JSONExtractFloat(raw_message, 'merchant_lon') as merchant_lon,
    JSONExtractUInt(raw_message, 'target') as target
FROM transactions_kafka_raw;