-- +goose Up
CREATE MATERIALIZED VIEW IF NOT EXISTS page_views_raw_to_minute
TO page_views_agg_minute
AS SELECT
    toStartOfMinute(event_time) AS window_start,
    page_id,
    sumState(1) as view_count,
    sumState(duration_ms) as total_duration,
    uniqState(user_id) as unique_users,
    sumState(is_bounce) as bounce_count
FROM page_views_raw
GROUP BY window_start, page_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS page_views_minute_to_hour
TO page_views_agg_hour
AS SELECT
    toStartOfHour(window_start) AS window_start,
    page_id,
    sum(merge(view_count)) as view_count,
    avg(merge(total_duration) / merge(view_count)) as avg_duration,
    uniq(merge(unique_users)) as unique_users,
    sum(merge(bounce_count)) * 100.0 / sum(merge(view_count)) as bounce_rate
FROM page_views_agg_minute
GROUP BY window_start, page_id;

-- +goose Down
DROP MATERIALIZED VIEW IF EXISTS page_views_minute_to_hour;
DROP MATERIALIZED VIEW IF EXISTS page_views_raw_to_minute;