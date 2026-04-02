include .env

.PHONY: up down logs migrate clean

up:
	docker compose up -d zookeeper kafka clickhouse
	until curl -s http://localhost:8123/ping >/dev/null 2>&1; do sleep 1; done
	goose -dir consumer/migrations clickhouse "$(CLICKHOUSE_DSN_LOCAL)" up
	docker compose up -d producer consumer

down:
	docker compose down

clean:
	docker compose down -v

logs:
	docker compose logs -f

migrate:
	goose -dir consumer/migrations clickhouse "$(CLICKHOUSE_DSN_LOCAL)" up