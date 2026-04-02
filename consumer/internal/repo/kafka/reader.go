package kafka

import (
	"context"

	kafkaGo "github.com/segmentio/kafka-go"
	"github.com/totorialman/realtime-analytics-pipeline/consumer/internal/config"
)

type Reader struct {
	reader *kafkaGo.Reader
}

func NewReader(cfg config.Config) *Reader {
	return &Reader{
		reader: kafkaGo.NewReader(kafkaGo.ReaderConfig{
			Brokers:        cfg.Brokers,
			GroupID:        cfg.GroupID,
			Topic:          cfg.Topic,
			MinBytes:       1,
			MaxBytes:       10e6,
			CommitInterval: 0,
			StartOffset:    kafkaGo.FirstOffset,
		}),
	}
}

func (r *Reader) FetchMessage(ctx context.Context) (kafkaGo.Message, error) {
	return r.reader.FetchMessage(ctx)
}

func (r *Reader) CommitMessages(ctx context.Context, messages ...kafkaGo.Message) error {
	return r.reader.CommitMessages(ctx, messages...)
}

func (r *Reader) Close() error {
	return r.reader.Close()
}