package clickhouse

import (
	"context"
	"errors"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/totorialman/realtime-analytics-pipeline/consumer/internal/config"
	"github.com/totorialman/realtime-analytics-pipeline/consumer/internal/domain"
	"github.com/totorialman/realtime-analytics-pipeline/shared"
)

type Repository struct {
	conn ch.Conn
}

func NewRepository(cfg config.Config) (*Repository, error) {
	conn, err := ch.Open(&ch.Options{
		Addr: strings.Split(cfg.ClickhouseDSN, ","),
		Auth: ch.Auth{
			Database: cfg.ClickhouseDB,
			Username: cfg.ClickhouseUser,
			Password: cfg.ClickhousePassword,
		},
		Protocol: ch.Native,
	})
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		return nil, err
	}

	return &Repository{conn: conn}, nil
}

func (r *Repository) InsertRawRows(ctx context.Context, rows []domain.RawRow) error {
	return retry(ctx, func() error {
		batch, err := r.conn.PrepareBatch(ctx, `
			INSERT INTO page_views_raw
			(event_time, page_id, user_id, duration_ms, user_agent, ip_address, region, is_bounce, kafka_offset, kafka_partition)
		`)
		if err != nil {
			return err
		}

		for _, row := range rows {
			if err := batch.Append(
				row.EventTime,
				row.PageID,
				row.UserID,
				row.DurationMS,
				row.UserAgent,
				row.IPAddress,
				row.Region,
				row.IsBounce,
				row.KafkaOffset,
				row.KafkaPartition,
			); err != nil {
				return err
			}
		}

		return batch.Send()
	})
}

func (r *Repository) InsertErrors(ctx context.Context, rows []domain.ErrorRow) error {
	return retry(ctx, func() error {
		batch, err := r.conn.PrepareBatch(ctx, `
			INSERT INTO processing_errors
			(error_time, raw_message, error_reason, kafka_offset, kafka_partition)
		`)
		if err != nil {
			return err
		}

		for _, row := range rows {
			if err := batch.Append(
				row.ErrorTime,
				row.RawMessage,
				row.ErrorReason,
				row.KafkaOffset,
				row.KafkaPartition,
			); err != nil {
				return err
			}
		}

		return batch.Send()
	})
}

func retry(ctx context.Context, fn func() error) error {
	backoff := 100 * time.Millisecond
	for i := 0; i < shared.MaxRetries; i++ {
		err := fn()
		if err == nil {
			return nil
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		time.Sleep(backoff)
		backoff *= 2
		if backoff > 10*time.Second {
			backoff = 10 * time.Second
		}
	}
	return errors.New("clickhouse operation failed after retries")
}