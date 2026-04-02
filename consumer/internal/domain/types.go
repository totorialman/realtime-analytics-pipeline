package domain

import (
	"net"
	"sync/atomic"
	"time"

	"github.com/totorialman/realtime-analytics-pipeline/shared"
)

type Metrics struct {
	ConsumedTotal  atomic.Uint64
	CommittedTotal atomic.Uint64
	ErrorTotal     atomic.Uint64
	DLQTotal       atomic.Uint64
}

type RawRow struct {
	EventTime      time.Time
	PageID         string
	UserID         string
	DurationMS     uint32
	UserAgent      string
	IPAddress      net.IP
	Region         string
	IsBounce       uint8
	KafkaOffset    int64
	KafkaPartition int32
}

type ErrorRow struct {
	ErrorTime      time.Time
	RawMessage     string
	ErrorReason    string
	KafkaOffset    int64
	KafkaPartition int32
}

type ValidatedEvent struct {
	Event          shared.PageViewEvent
	KafkaOffset    int64
	KafkaPartition int32
	RawMessage     string
}