package domain

func (m *Metrics) IncConsumed(n uint64) {
	m.ConsumedTotal.Add(n)
}

func (m *Metrics) IncCommitted(n uint64) {
	m.CommittedTotal.Add(n)
}

func (m *Metrics) IncErrors(n uint64) {
	m.ErrorTotal.Add(n)
}

func (m *Metrics) IncDLQ(n uint64) {
	m.DLQTotal.Add(n)
}

func (m *Metrics) Snapshot() map[string]any {
	return map[string]any{
		"consumed_total":  m.ConsumedTotal.Load(),
		"committed_total": m.CommittedTotal.Load(),
		"errors_total":    m.ErrorTotal.Load(),
		"dlq_total":       m.DLQTotal.Load(),
	}
}
