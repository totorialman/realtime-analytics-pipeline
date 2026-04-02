.PHONY: start stop logs

CLICKHOUSE_HOST = localhost
CLICKHOUSE_PORT = 9000
CLICKHOUSE_DB = default
CLICKHOUSE_USER = default
CLICKHOUSE_PASSWORD =
CLICKHOUSE_DSN = tcp://$(CLICKHOUSE_HOST):$(CLICKHOUSE_PORT)

start:
	docker-compose up -d clickhouse kafka zookeeper
	@until curl -s http://localhost:8123/ping > /dev/null 2>&1; do sleep 1; done
	goose -dir consumer/migrations clickhouse "$(CLICKHOUSE_DSN)" up
	docker-compose up -d producer consumer

stop:
	@echo "🛑 Остановка..."
	docker-compose down

logs:
	docker-compose logs -f