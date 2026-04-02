package domain

import (
	"sync/atomic"
	"time"
)

type Metrics struct {
	SentTotal      atomic.Uint64
	ErrorTotal     atomic.Uint64
	DuplicateTotal atomic.Uint64
	LatencyNanos   atomic.Uint64
	LatencyCount   atomic.Uint64
}

func (m *Metrics) ObserveSent(latency time.Duration) {
	m.SentTotal.Add(1)
	m.LatencyNanos.Add(uint64(latency.Nanoseconds()))
	m.LatencyCount.Add(1)
}

func (m *Metrics) ObserveError() {
	m.ErrorTotal.Add(1)
}

func (m *Metrics) ObserveDuplicate() {
	m.DuplicateTotal.Add(1)
}

func (m *Metrics) AverageLatencyMs() float64 {
	count := m.LatencyCount.Load()
	if count == 0 {
		return 0
	}
	return float64(m.LatencyNanos.Load()) / float64(count) / float64(time.Millisecond)
}