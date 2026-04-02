package main

import (
	"context"
	"log"
	"net/http"

	"github.com/totorialman/realtime-analytics-pipeline/consumer/internal/service"
	"github.com/totorialman/realtime-analytics-pipeline/consumer/internal/config"
	"github.com/totorialman/realtime-analytics-pipeline/consumer/internal/domain"
	"github.com/totorialman/realtime-analytics-pipeline/consumer/internal/repo/clickhouse"
	kafka "github.com/totorialman/realtime-analytics-pipeline/consumer/internal/repo/kafka"
	"github.com/totorialman/realtime-analytics-pipeline/consumer/internal/handler"
)

func main() {
	cfg := config.MustLoad()

	metrics := &domain.Metrics{}
	reader := kafka.NewReader(cfg)
	defer reader.Close()

	repo, err := clickhouse.NewRepository(cfg)
	if err != nil {
		log.Fatal(err)
	}

	service := app.NewService(cfg, reader, repo, metrics)

	go func() {
		log.Fatal(service.Run(context.Background()))
	}()

	handler := handler.NewHandler(metrics)
	log.Fatal(http.ListenAndServe(cfg.HTTPAddr, handler))
}