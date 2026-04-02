package kafka

import (
	"context"
	"errors"
	"math/rand"
	"time"

	kafkaGo "github.com/segmentio/kafka-go"
	"github.com/totorialman/realtime-analytics-pipeline/producer/internal/config"
	"github.com/totorialman/realtime-analytics-pipeline/producer/internal/domain"
	"github.com/totorialman/realtime-analytics-pipeline/shared"
)

type Writer struct {
	cfg      config.Config
	metrics  *domain.Metrics
	syncW    map[shared.PartitionStrategy]*kafkaGo.Writer
	asyncW   map[shared.PartitionStrategy]*kafkaGo.Writer
	batchW   map[shared.PartitionStrategy]*kafkaGo.Writer
}

type randomBalancer struct{}

func (b *randomBalancer) Balance(msg kafkaGo.Message, partitions ...int) int {
	if len(partitions) == 0 {
		return 0
	}
	return partitions[rand.Intn(len(partitions))]
}

func NewWriter(cfg config.Config, metrics *domain.Metrics) *Writer {
	makeWriter := func(async bool, batchSize int, batchTimeout time.Duration, balancer kafkaGo.Balancer) *kafkaGo.Writer {
		return &kafkaGo.Writer{
			Addr:         kafkaGo.TCP(cfg.Brokers...),
			Topic:        cfg.Topic,
			RequiredAcks: kafkaGo.RequireAll,
			Async:        async,
			BatchSize:    batchSize,
			BatchTimeout: batchTimeout,
			Balancer:     balancer,
			Completion: func(messages []kafkaGo.Message, err error) {
				if !async {
					return
				}
				if err != nil {
					for range messages {
						metrics.ObserveError()
					}
					return
				}
				now := time.Now()
				for _, message := range messages {
					metrics.ObserveSent(now.Sub(message.Time))
				}
			},
		}
	}

	hashBalancer := &kafkaGo.Hash{}
	rrBalancer := &kafkaGo.RoundRobin{}
	random := &randomBalancer{}

	return &Writer{
		cfg:     cfg,
		metrics: metrics,
		syncW: map[shared.PartitionStrategy]*kafkaGo.Writer{
			shared.PartitionByKey:      makeWriter(false, 1, 0, hashBalancer),
			shared.PartitionRoundRobin: makeWriter(false, 1, 0, rrBalancer),
			shared.PartitionRandom:     makeWriter(false, 1, 0, random),
		},
		asyncW: map[shared.PartitionStrategy]*kafkaGo.Writer{
			shared.PartitionByKey:      makeWriter(true, 1, 0, hashBalancer),
			shared.PartitionRoundRobin: makeWriter(true, 1, 0, rrBalancer),
			shared.PartitionRandom:     makeWriter(true, 1, 0, random),
		},
		batchW: map[shared.PartitionStrategy]*kafkaGo.Writer{
			shared.PartitionByKey:      makeWriter(false, cfg.BatchSize, cfg.BatchTimeout, hashBalancer),
			shared.PartitionRoundRobin: makeWriter(false, cfg.BatchSize, cfg.BatchTimeout, rrBalancer),
			shared.PartitionRandom:     makeWriter(false, cfg.BatchSize, cfg.BatchTimeout, random),
		},
	}
}

func (w *Writer) Close() {
	seen := map[*kafkaGo.Writer]struct{}{}
	for _, item := range w.syncW {
		seen[item] = struct{}{}
	}
	for _, item := range w.asyncW {
		seen[item] = struct{}{}
	}
	for _, item := range w.batchW {
		seen[item] = struct{}{}
	}
	for item := range seen {
		_ = item.Close()
	}
}

func (w *Writer) SendSync(ctx context.Context, strategy shared.PartitionStrategy, msg domain.ProducedMessage) error {
	return w.send(ctx, w.syncW[strategy], msg)
}

func (w *Writer) SendAsync(ctx context.Context, strategy shared.PartitionStrategy, msg domain.ProducedMessage) error {
	return w.send(ctx, w.asyncW[strategy], msg)
}

func (w *Writer) SendBatch(ctx context.Context, strategy shared.PartitionStrategy, messages []domain.ProducedMessage) error {
	kafkaMessages := make([]kafkaGo.Message, 0, len(messages))
	now := time.Now().UTC()
	for _, msg := range messages {
		kafkaMessages = append(kafkaMessages, kafkaGo.Message{
			Key:   msg.Key,
			Value: msg.Value,
			Time:  now,
		})
	}
	return w.writeWithRetry(ctx, w.batchW[strategy], kafkaMessages)
}

func (w *Writer) send(ctx context.Context, writer *kafkaGo.Writer, msg domain.ProducedMessage) error {
	return w.writeWithRetry(ctx, writer, []kafkaGo.Message{
		{
			Key:   msg.Key,
			Value: msg.Value,
			Time:  time.Now().UTC(),
		},
	})
}

func (w *Writer) writeWithRetry(ctx context.Context, writer *kafkaGo.Writer, messages []kafkaGo.Message) error {
	backoff := 100 * time.Millisecond
	start := time.Now()
	for i := 0; i < shared.MaxRetries; i++ {
		err := writer.WriteMessages(ctx, messages...)
		if err == nil {
			if !writer.Async {
				for range messages {
					w.metrics.ObserveSent(time.Since(start))
				}
			}
			return nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			for range messages {
				w.metrics.ObserveError()
			}
			return err
		}
		time.Sleep(backoff)
		backoff *= 2
		if backoff > 10*time.Second {
			backoff = 10 * time.Second
		}
	}
	for range messages {
		w.metrics.ObserveError()
	}
	return errors.New("kafka write failed after retries")
}