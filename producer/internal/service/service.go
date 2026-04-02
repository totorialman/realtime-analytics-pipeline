package service

import (
	"context"
	"math/rand"
	"time"

	"github.com/totorialman/realtime-analytics-pipeline/producer/internal/config"
	"github.com/totorialman/realtime-analytics-pipeline/producer/internal/domain"
	kafkainfra "github.com/totorialman/realtime-analytics-pipeline/producer/internal/infra/kafka"
	shared "github.com/totorialman/realtime-analytics-pipeline/shared"
)

type Service struct {
	cfg       config.Config
	state     *domain.RuntimeState
	metrics   *domain.Metrics
	writer    *kafkainfra.Writer
	generator *domain.Generator
	batcher   *domain.Batcher
}

func NewService(cfg config.Config, state *domain.RuntimeState, metrics *domain.Metrics, writer *kafkainfra.Writer) *Service {
	s := &Service{
		cfg:       cfg,
		state:     state,
		metrics:   metrics,
		writer:    writer,
		generator: domain.NewGenerator(),
	}
	s.batcher = domain.NewBatcher(cfg.BatchSize, cfg.BatchTimeout, batchSender{s: s})
	return s
}

type batchSender struct {
	s *Service
}

func (b batchSender) SendBatch(ctx context.Context, messages []domain.ProducedMessage) error {
	return b.s.writer.SendBatch(ctx, b.s.state.Partition(), messages)
}

func (s *Service) Run(ctx context.Context) {
	go s.batcher.Run(ctx)

	for {
		switch s.state.GenerationMode() {
		case shared.ModeRegular:
			s.runRegular(ctx)
		case shared.ModeBurst:
			s.runBurst(ctx)
		case shared.ModeNight:
			s.runNight(ctx)
		default:
			s.state.SetGenerationMode(shared.ModeRegular)
		}
	}
}

func (s *Service) runRegular(ctx context.Context) {
	rate := s.state.RegularRate()
	if rate < 1 {
		rate = 1
	}
	if rate > 10 {
		rate = 10
	}

	ticker := time.NewTicker(time.Second / time.Duration(rate))
	defer ticker.Stop()

	timeout := time.After(2 * time.Second)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.emitOne(ctx)
		case <-timeout:
			return
		}
	}
}

func (s *Service) runBurst(ctx context.Context) {
	total := rand.Intn(901) + 100
	deadline := time.Now().Add(2 * time.Second)

	for i := 0; i < total; i++ {
		s.emitOne(ctx)
		left := total - i - 1
		if left > 0 {
			remaining := time.Until(deadline)
			if remaining > 0 {
				time.Sleep(remaining / time.Duration(left))
			}
		}
	}

	select {
	case <-ctx.Done():
		return
	case <-time.After(s.cfg.BurstInterval):
	}
}

func (s *Service) runNight(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(10 * time.Second):
		s.emitOne(ctx)
	}
}

func (s *Service) emitOne(ctx context.Context) {
	msg := s.generator.Next()

	if rand.Intn(100) == 0 {
		s.metrics.ObserveDuplicate()
	}

	switch s.state.SendMode() {
	case shared.SendModeSync:
		_ = s.writer.SendSync(ctx, s.state.Partition(), msg)
	case shared.SendModeAsync:
		_ = s.writer.SendAsync(ctx, s.state.Partition(), msg)
	case shared.SendModeBatch:
		s.batcher.Input() <- msg
	default:
		_ = s.writer.SendSync(ctx, s.state.Partition(), msg)
	}
}

func (s *Service) Update(gen shared.GenerationMode, send shared.SendMode, partition shared.PartitionStrategy, rate int) {
	if gen != "" {
		s.state.SetGenerationMode(gen)
	}
	if send != "" {
		s.state.SetSendMode(send)
	}
	if partition != "" {
		s.state.SetPartition(partition)
	}
	if rate > 0 {
		s.state.SetRegularRate(rate)
	}
}

func (s *Service) Snapshot() map[string]any {
	return map[string]any{
		"generation_mode":    s.state.GenerationMode(),
		"send_mode":          s.state.SendMode(),
		"partition_strategy": s.state.Partition(),
		"regular_rate":       s.state.RegularRate(),
	}
}