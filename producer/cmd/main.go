package main

import (
	"context"
	"log"
	"net/http"

	"github.com/totorialman/realtime-analytics-pipeline/producer/internal/service"
	"github.com/totorialman/realtime-analytics-pipeline/producer/internal/config"
	"github.com/totorialman/realtime-analytics-pipeline/producer/internal/domain"
	"github.com/totorialman/realtime-analytics-pipeline/producer/internal/repo/kafka"
	"github.com/totorialman/realtime-analytics-pipeline/producer/internal/handler"
)

func main() {
	cfg := config.MustLoad()

	metrics := &domain.Metrics{}
	state := domain.NewRuntimeState(cfg.GenerationMode, cfg.SendMode, cfg.PartitionStrategy, cfg.RegularRate)

	writer := kafka.NewWriter(cfg, metrics)
	defer writer.Close()

	service := service.NewService(cfg, state, metrics, writer)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go service.Run(ctx)

	handler := handler.NewHandler(service, metrics)
	log.Fatal(http.ListenAndServe(cfg.HTTPAddr, handler))
}