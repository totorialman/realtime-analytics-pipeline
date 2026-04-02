package app

import (
	"context"
	"errors"
	"time"

	kafkaGo "github.com/segmentio/kafka-go"
	"github.com/totorialman/realtime-analytics-pipeline/consumer/internal/config"
	"github.com/totorialman/realtime-analytics-pipeline/consumer/internal/domain"
)

type Reader interface {
	FetchMessage(context.Context) (kafkaGo.Message, error)
	CommitMessages(context.Context, ...kafkaGo.Message) error
}

type Repository interface {
	InsertRawRows(context.Context, []domain.RawRow) error
	InsertErrors(context.Context, []domain.ErrorRow) error
}

type Service struct {
	cfg     config.Config
	reader  Reader
	repo    Repository
	metrics *domain.Metrics
}

func NewService(cfg config.Config, reader Reader, repo Repository, metrics *domain.Metrics) *Service {
	return &Service{
		cfg:     cfg,
		reader:  reader,
		repo:    repo,
		metrics: metrics,
	}
}

func (s *Service) Run(ctx context.Context) error {
	switch s.cfg.ConsumerMode {
	case "batch":
		return s.runBatch(ctx)
	case "time":
		return s.runTime(ctx)
	default:
		return s.runHybrid(ctx)
	}
}

func (s *Service) runBatch(ctx context.Context) error {
	buffer := make([]kafkaGo.Message, 0, s.cfg.BatchSize)

	for {
		msg, err := s.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		s.metrics.ConsumedTotal.Add(1)
		buffer = append(buffer, msg)

		if len(buffer) >= s.cfg.BatchSize {
			if err := s.processBatch(ctx, buffer); err != nil {
				return err
			}
			buffer = buffer[:0]
		}
	}
}

func (s *Service) runTime(ctx context.Context) error {
	msgCh := make(chan kafkaGo.Message, s.cfg.BatchSize*4)
	errCh := make(chan error, 1)

	go func() {
		for {
			msg, err := s.reader.FetchMessage(ctx)
			if err != nil {
				errCh <- err
				return
			}
			s.metrics.ConsumedTotal.Add(1)
			msgCh <- msg
		}
	}()

	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()

	buffer := make([]kafkaGo.Message, 0, s.cfg.BatchSize)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			return err
		case msg := <-msgCh:
			buffer = append(buffer, msg)
		case <-ticker.C:
			if len(buffer) == 0 {
				continue
			}
			if err := s.processBatch(ctx, buffer); err != nil {
				return err
			}
			buffer = buffer[:0]
		}
	}
}

func (s *Service) runHybrid(ctx context.Context) error {
	msgCh := make(chan kafkaGo.Message, s.cfg.BatchSize*4)
	errCh := make(chan error, 1)

	go func() {
		for {
			msg, err := s.reader.FetchMessage(ctx)
			if err != nil {
				errCh <- err
				return
			}
			s.metrics.ConsumedTotal.Add(1)
			msgCh <- msg
		}
	}()

	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()

	buffer := make([]kafkaGo.Message, 0, s.cfg.BatchSize)

	flush := func() error {
		if len(buffer) == 0 {
			return nil
		}
		if err := s.processBatch(ctx, buffer); err != nil {
			return err
		}
		buffer = buffer[:0]
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			return err
		case msg := <-msgCh:
			buffer = append(buffer, msg)
			if len(buffer) >= s.cfg.BatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-ticker.C:
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

func (s *Service) processBatch(ctx context.Context, messages []kafkaGo.Message) error {
	rawRows := make([]domain.RawRow, 0, len(messages))
	errorRows := make([]domain.ErrorRow, 0)

	for _, msg := range messages {
		validated, err := domain.Validate(msg.Value, msg.Offset, int32(msg.Partition))
		if err != nil {
			errorRows = append(errorRows, domain.ToErrorRow(msg.Value, err.Error(), msg.Offset, int32(msg.Partition)))
			continue
		}
		rawRows = append(rawRows, domain.ToRawRow(validated))
	}

	if len(errorRows) > 0 {
		if err := s.repo.InsertErrors(ctx, errorRows); err != nil {
			s.metrics.ErrorTotal.Add(uint64(len(errorRows)))
			return err
		}
		s.metrics.DLQTotal.Add(uint64(len(errorRows)))
	}

	if len(rawRows) > 0 {
		if err := s.repo.InsertRawRows(ctx, rawRows); err != nil {
			s.metrics.ErrorTotal.Add(uint64(len(rawRows)))
			return err
		}
	}

	if len(messages) == 0 {
		return errors.New("empty batch")
	}

	if err := s.reader.CommitMessages(ctx, messages...); err != nil {
		s.metrics.ErrorTotal.Add(uint64(len(messages)))
		return err
	}

	s.metrics.CommittedTotal.Add(uint64(len(messages)))
	return nil
}