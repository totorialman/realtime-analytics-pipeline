package main

import (
	"context"
	"log"
	stdhttp "net/http"

	"github.com/totorialman/realtime-analytics-pipeline/consumer/internal/app"
	"github.com/totorialman/realtime-analytics-pipeline/consumer/internal/config"
	"github.com/totorialman/realtime-analytics-pipeline/consumer/internal/domain"
	chinfra "github.com/totorialman/realtime-analytics-pipeline/consumer/internal/infra/clickhouse"
	kafkainfra "github.com/totorialman/realtime-analytics-pipeline/consumer/internal/infra/kafka"
	httptransport "github.com/totorialman/realtime-analytics-pipeline/consumer/internal/transport/http"
)

func main() {
	cfg := config.MustLoad()

	metrics := &domain.Metrics{}
	reader := kafkainfra.NewReader(cfg)
	defer reader.Close()

	repo, err := chinfra.NewRepository(cfg)
	if err != nil {
		log.Fatal(err)
	}

	service := app.NewService(cfg, reader, repo, metrics)

	go func() {
		log.Fatal(service.Run(context.Background()))
	}()

	handler := httptransport.NewHandler(metrics)
	log.Fatal(stdhttp.ListenAndServe(cfg.HTTPAddr, handler))
}