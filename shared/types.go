package shared

import (
	"encoding/json"
	"time"
)

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

func (e PageViewEvent) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}

func PageViewEventFromJSON(data []byte) (PageViewEvent, error) {
	var event PageViewEvent
	err := json.Unmarshal(data, &event)
	return event, err
}

type GenerationMode string

const (
	ModeRegular GenerationMode = "regular"
	ModeBurst   GenerationMode = "burst"
	ModeNight   GenerationMode = "night"
)

type SendMode string

const (
	SendModeSync  SendMode = "sync"
	SendModeAsync SendMode = "async"
	SendModeBatch SendMode = "batch"
)

type PartitionStrategy string

const (
	PartitionByKey      PartitionStrategy = "key"
	PartitionRoundRobin PartitionStrategy = "round_robin"
	PartitionRandom     PartitionStrategy = "random"
)

const (
	TopicPageViews = "page_views"
	MaxRetries     = 5
)