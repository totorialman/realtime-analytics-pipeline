-- +goose Up
CREATE MATERIALIZED VIEW IF NOT EXISTS page_views_raw_to_minute
TO page_views_agg_minute
AS
SELECT
    toStartOfMinute(event_time) AS window_start,
    page_id,
    sumState(toUInt64(1)) AS view_count,
    sumState(toUInt64(duration_ms)) AS total_duration,
    uniqState(user_id) AS unique_users,
    sumState(is_bounce) AS bounce_count
FROM page_views_raw
GROUP BY window_start, page_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS page_views_raw_to_hour
TO page_views_agg_hour
AS
SELECT
    toStartOfHour(event_time) AS window_start,
    page_id,
    sumState(toUInt64(1)) AS view_count,
    sumState(toUInt64(duration_ms)) AS total_duration,
    uniqState(user_id) AS unique_users,
    sumState(is_bounce) AS bounce_count
FROM page_views_raw
GROUP BY window_start, page_id;

-- +goose Down
DROP VIEW IF EXISTS page_views_raw_to_hour;
DROP VIEW IF EXISTS page_views_raw_to_minute;