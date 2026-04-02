package shared

import (
	"encoding/json"
	"time"
)

// PageViewEvent представляет событие просмотра страницы
type PageViewEvent struct {
	PageID       string    `json:"page_id"`
	UserID       string    `json:"user_id"`
	ViewDuration int       `json:"view_duration_ms"`
	Timestamp    time.Time `json:"timestamp"`
	UserAgent    string    `json:"user_agent,omitempty"`
	IPAddress    string    `json:"ip_address,omitempty"`
	Region       string    `json:"region,omitempty"`
	IsBounce     bool      `json:"is_bounce"`
}

// ToJSON сериализует событие в JSON
func (e *PageViewEvent) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}

// PageViewEventFromJSON десериализует событие из JSON
func PageViewEventFromJSON(data []byte) (*PageViewEvent, error) {
	var event PageViewEvent
	err := json.Unmarshal(data, &event)
	return &event, err
}

// GenerationMode определяет режим генерации событий
type GenerationMode int

const (
	ModeRegular GenerationMode = iota
	ModeBurst
	ModeNight
)

// PartitionStrategy определяет стратегию партиционирования
type PartitionStrategy int

const (
	PartitionByKey PartitionStrategy = iota
	PartitionRoundRobin
	PartitionRandom
)

// Константы для работы с Kafka
const (
	TopicPageViews = "page_views"
	MaxRetries     = 5
	InitialBackoff = 100 * time.Millisecond
	MaxBackoff     = 10 * time.Second
)