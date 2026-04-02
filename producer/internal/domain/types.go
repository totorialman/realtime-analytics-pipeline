package domain

import (
	"sync/atomic"

	"github.com/totorialman/realtime-analytics-pipeline/shared"
)

type RuntimeState struct {
	generationMode atomic.Value
	sendMode       atomic.Value
	partition      atomic.Value
	regularRate    atomic.Int64
}

func NewRuntimeState(gen shared.GenerationMode, send shared.SendMode, partition shared.PartitionStrategy, rate int) *RuntimeState {
	s := &RuntimeState{}
	s.generationMode.Store(gen)
	s.sendMode.Store(send)
	s.partition.Store(partition)
	s.regularRate.Store(int64(rate))
	return s
}

func (s *RuntimeState) GenerationMode() shared.GenerationMode {
	return s.generationMode.Load().(shared.GenerationMode)
}

func (s *RuntimeState) SendMode() shared.SendMode {
	return s.sendMode.Load().(shared.SendMode)
}

func (s *RuntimeState) Partition() shared.PartitionStrategy {
	return s.partition.Load().(shared.PartitionStrategy)
}

func (s *RuntimeState) RegularRate() int {
	return int(s.regularRate.Load())
}

func (s *RuntimeState) SetGenerationMode(v shared.GenerationMode) {
	s.generationMode.Store(v)
}

func (s *RuntimeState) SetSendMode(v shared.SendMode) {
	s.sendMode.Store(v)
}

func (s *RuntimeState) SetPartition(v shared.PartitionStrategy) {
	s.partition.Store(v)
}

func (s *RuntimeState) SetRegularRate(v int) {
	s.regularRate.Store(int64(v))
}

type ProducedMessage struct {
	Key   []byte
	Value []byte
}