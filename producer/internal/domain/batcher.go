package domain

import (
	"context"
	"time"
)

type BatchSender interface {
	SendBatch(context.Context, []ProducedMessage) error
}

type Batcher struct {
	size    int
	timeout time.Duration
	in      chan ProducedMessage
	sender  BatchSender
}

func NewBatcher(size int, timeout time.Duration, sender BatchSender) *Batcher {
	return &Batcher{
		size:    size,
		timeout: timeout,
		in:      make(chan ProducedMessage, size*4),
		sender:  sender,
	}
}

func (b *Batcher) Input() chan<- ProducedMessage {
	return b.in
}

func (b *Batcher) Run(ctx context.Context) {
	timer := time.NewTimer(b.timeout)
	defer timer.Stop()

	buf := make([]ProducedMessage, 0, b.size)

	flush := func() {
		if len(buf) == 0 {
			return
		}
		_ = b.sender.SendBatch(ctx, buf)
		buf = buf[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case msg := <-b.in:
			buf = append(buf, msg)
			if len(buf) >= b.size {
				flush()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(b.timeout)
			}
		case <-timer.C:
			flush()
			timer.Reset(b.timeout)
		}
	}
}