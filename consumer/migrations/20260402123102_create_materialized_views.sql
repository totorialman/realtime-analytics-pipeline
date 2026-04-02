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

CREATE MATERIALIZED VIEW IF NOT EXISTS page_views_minute_to_hour
TO page_views_agg_hour
AS
SELECT
    toStartOfHour(window_start) AS window_start,
    page_id,
    sumMergeState(view_count) AS view_count,
    sumMergeState(total_duration) AS total_duration,
    uniqMergeState(unique_users) AS unique_users,
    sumMergeState(bounce_count) AS bounce_count
FROM page_views_agg_minute
GROUP BY window_start, page_id;

-- +goose Down
DROP MATERIALIZED VIEW IF EXISTS page_views_minute_to_hour;
DROP MATERIALIZED VIEW IF EXISTS page_views_raw_to_minute;