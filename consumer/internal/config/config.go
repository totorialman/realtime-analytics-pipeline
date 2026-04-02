package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Brokers       []string
	Topic         string
	GroupID       string
	ClickhouseDSN string
	ClickhouseDB  string
	ClickhouseUser string
	ClickhousePassword string
	ConsumerMode  string
	BatchSize     int
	FlushInterval time.Duration
	HTTPAddr      string
}

func MustLoad() Config {
	return Config{
		Brokers:            splitCSV(getEnv("KAFKA_BROKERS", "kafka:9092")),
		Topic:              getEnv("KAFKA_TOPIC", "page_views"),
		GroupID:            getEnv("KAFKA_GROUP_ID", "page-views-consumer"),
		ClickhouseDSN:      getEnv("CLICKHOUSE_DSN", "clickhouse:9000"),
		ClickhouseDB:       getEnv("CLICKHOUSE_DB", "default"),
		ClickhouseUser:     getEnv("CLICKHOUSE_USER", "default"),
		ClickhousePassword: getEnv("CLICKHOUSE_PASSWORD", ""),
		ConsumerMode:       getEnv("CONSUMER_MODE", "hybrid"),
		BatchSize:          getEnvInt("BATCH_SIZE", 1000),
		FlushInterval:      time.Duration(getEnvInt("FLUSH_INTERVAL_SEC", 5)) * time.Second,
		HTTPAddr:           getEnv("HTTP_ADDR", ":8081"),
	}
}

func getEnv(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

func getEnvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func splitCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}