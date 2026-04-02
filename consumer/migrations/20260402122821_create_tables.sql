-- +goose Up
CREATE TABLE IF NOT EXISTS page_views_raw
(
    event_date       Date DEFAULT today(),
    event_time       DateTime64(3, 'UTC'),
    page_id          String,
    user_id          String,
    duration_ms      UInt32,
    user_agent       String,
    ip_address       IPv6,
    region           LowCardinality(String),
    is_bounce        UInt8,
    kafka_offset     Int64,
    kafka_partition  Int32,
    processed_time   DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, page_id, user_id)
TTL event_date + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS processing_errors
(
    error_time       DateTime,
    raw_message      String,
    error_reason     String,
    kafka_offset     Int64,
    kafka_partition  Int32
) ENGINE = MergeTree()
ORDER BY (error_time);

-- +goose Down
DROP TABLE IF EXISTS processing_errors;
DROP TABLE IF EXISTS page_views_raw;