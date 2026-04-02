package config

import (
	"os"
	"strconv"
	"strings"
	"time"

	shared "github.com/totorialman/realtime-analytics-pipeline/shared"
)

type Config struct {
	Brokers           []string
	Topic             string
	HTTPAddr          string
	GenerationMode    shared.GenerationMode
	SendMode          shared.SendMode
	PartitionStrategy shared.PartitionStrategy
	RegularRate       int
	BatchSize         int
	BatchTimeout      time.Duration
	BurstInterval     time.Duration
}

func MustLoad() Config {
	return Config{
		Brokers:           splitCSV(getEnv("KAFKA_BROKERS", "kafka:9092")),
		Topic:             getEnv("KAFKA_TOPIC", shared.TopicPageViews),
		HTTPAddr:          getEnv("HTTP_ADDR", ":8080"),
		GenerationMode:    shared.GenerationMode(getEnv("GENERATION_MODE", string(shared.ModeRegular))),
		SendMode:          shared.SendMode(getEnv("SEND_MODE", string(shared.SendModeBatch))),
		PartitionStrategy: shared.PartitionStrategy(getEnv("PARTITION_STRATEGY", string(shared.PartitionByKey))),
		RegularRate:       getEnvInt("REGULAR_RATE", 5),
		BatchSize:         getEnvInt("BATCH_SIZE", 200),
		BatchTimeout:      time.Duration(getEnvInt("BATCH_TIMEOUT_MS", 1500)) * time.Millisecond,
		BurstInterval:     time.Duration(getEnvInt("BURST_INTERVAL_SEC", 10)) * time.Second,
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