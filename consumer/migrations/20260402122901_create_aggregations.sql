-- +goose Up
CREATE TABLE IF NOT EXISTS page_views_agg_minute
(
    window_start   DateTime,
    page_id        String,
    view_count     AggregateFunction(sum, UInt64),
    total_duration AggregateFunction(sum, UInt64),
    unique_users   AggregateFunction(uniq, String),
    bounce_count   AggregateFunction(sum, UInt8)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(window_start)
ORDER BY (window_start, page_id)
TTL window_start + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS page_views_agg_hour
(
    window_start  DateTime,
    page_id       String,
    view_count    UInt64,
    avg_duration  Float32,
    unique_users  UInt64,
    bounce_rate   Float32
) ENGINE = SummingMergeTree()
ORDER BY (window_start, page_id);

-- +goose Down
DROP TABLE IF EXISTS page_views_agg_hour;
DROP TABLE IF EXISTS page_views_agg_minute;