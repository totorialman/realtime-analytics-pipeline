# realtime-analytics-pipeline

# Запуск проекта

```bash
# Запустит все сервисы + применит миграции
make up
```

---

# Проверка сервисов

## Producer
```bash
curl http://localhost:8080/health
curl http://localhost:8080/metrics
```
## Consumer
```bash
curl http://localhost:8081/health
curl http://localhost:8081/metrics
```
Метрики:

* Producer: sent_total, errors_total, duplicates_total, avg_latency_ms
* Consumer: consumed_total, committed_total, dlq_total

---

# Изменение режима генерации
```bash
curl -X POST http://localhost:8080/rate \
  -H "Content-Type: application/json" \
  -d '{
    "generation_mode":"burst",
    "send_mode":"async",
    "partition_strategy":"round_robin"
  }'
```
Режимы:

* generation_mode: regular | burst | night
* send_mode: sync | async | batch
* partition_strategy: key | round_robin | random

---

# Проверка ClickHouse

Подключение:
```bash
docker exec -it realtime-analytics-pipeline-clickhouse-1 \
clickhouse-client --user admin --password strongpassword123 --database analytics_db
```
## Сырые события
```bash
SELECT count() FROM page_views_raw;
```
## Ошибки (DLQ)
```bash
SELECT
    error_reason,
    count()
FROM processing_errors
GROUP BY error_reason
ORDER BY count() DESC;
```
## Минутная агрегация
```bash
SELECT
    window_start,
    page_id,
    views AS view_count,
    total_duration,
    unique_users,
    bounce_count
FROM
(
    SELECT
        window_start,
        page_id,
        sumMerge(view_count) AS views,
        sumMerge(total_duration) AS total_duration,
        uniqMerge(unique_users) AS unique_users,
        sumMerge(bounce_count) AS bounce_count
    FROM page_views_agg_minute
    GROUP BY window_start, page_id
)
ORDER BY window_start DESC
LIMIT 10;
```
## Часовая агрегация
```bash
SELECT
    window_start,
    page_id,
    views AS view_count,
    total_duration / views AS avg_duration,
    unique_users,
    bounce_count * 100.0 / views AS bounce_rate
FROM
(
    SELECT
        window_start,
        page_id,
        sumMerge(view_count) AS views,
        sumMerge(total_duration) AS total_duration,
        uniqMerge(unique_users) AS unique_users,
        sumMerge(bounce_count) AS bounce_count
    FROM page_views_agg_hour
    GROUP BY window_start, page_id
)
ORDER BY window_start DESC
LIMIT 10;
```
---
