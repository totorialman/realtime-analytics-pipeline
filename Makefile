include .env

.PHONY: up down logs migrate clean

up:
	docker compose up -d zookeeper kafka clickhouse
	@echo "Waiting for ClickHouse to be ready..."
	until curl -s http://localhost:8123/ping >/dev/null 2>&1; do sleep 1; done
	@echo "ClickHouse is ready. Running migrations..."
	goose -dir consumer/migrations clickhouse "$(CLICKHOUSE_DSN_LOCAL)" up
	@echo "Migrations complete. Starting producer and consumer..."
	docker compose up -d producer consumer

down:
	docker compose down

clean:
	docker compose down -v

logs:
	docker compose logs -f

migrate:
	goose -dir consumer/migrations clickhouse "$(CLICKHOUSE_DSN_LOCAL)" up