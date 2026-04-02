package domain

import (
	"errors"
	"net"
	"time"

	"github.com/totorialman/realtime-analytics-pipeline/shared"
)

func Validate(raw []byte, offset int64, partition int32) (ValidatedEvent, error) {
	event, err := shared.PageViewEventFromJSON(raw)
	if err != nil {
		return ValidatedEvent{}, errors.New("invalid_json")
	}
	if event.PageID == "" {
		return ValidatedEvent{}, errors.New("empty_page_id")
	}
	if event.ViewDuration <= 0 {
		return ValidatedEvent{}, errors.New("negative_or_zero_duration")
	}
	if event.Timestamp.IsZero() {
		return ValidatedEvent{}, errors.New("empty_timestamp")
	}
	return ValidatedEvent{
		Event:          event,
		KafkaOffset:    offset,
		KafkaPartition: partition,
		RawMessage:     string(raw),
	}, nil
}

func ToRawRow(v ValidatedEvent) RawRow {
	ip := net.ParseIP(v.Event.IPAddress)
	if ip == nil {
		ip = net.ParseIP("::ffff:127.0.0.1")
	}

	return RawRow{
		EventTime:      v.Event.Timestamp.UTC(),
		PageID:         v.Event.PageID,
		UserID:         v.Event.UserID,
		DurationMS:     uint32(v.Event.ViewDuration),
		UserAgent:      v.Event.UserAgent,
		IPAddress:      ip.To16(),
		Region:         v.Event.Region,
		IsBounce:       boolToUint8(v.Event.IsBounce),
		KafkaOffset:    v.KafkaOffset,
		KafkaPartition: v.KafkaPartition,
	}
}

func ToErrorRow(raw []byte, reason string, offset int64, partition int32) ErrorRow {
	return ErrorRow{
		ErrorTime:      time.Now().UTC(),
		RawMessage:     string(raw),
		ErrorReason:    reason,
		KafkaOffset:    offset,
		KafkaPartition: partition,
	}
}

func boolToUint8(v bool) uint8 {
	if v {
		return 1
	}
	return 0
}