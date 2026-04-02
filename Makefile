.PHONY: up down logs migrate

CLICKHOUSE_DSN=tcp://localhost:9000

up:
	docker compose up -d zookeeper kafka clickhouse
	until curl -s http://localhost:8123/ping >/dev/null 2>&1; do sleep 1; done
	goose -dir consumer/migrations clickhouse "$(CLICKHOUSE_DSN)" up
	docker compose up -d producer consumer

down:
	docker compose down -v

logs:
	docker compose logs -f

migrate:
	goose -dir consumer/migrations clickhouse "$(CLICKHOUSE_DSN)" up