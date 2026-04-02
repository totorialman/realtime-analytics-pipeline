package domain

import (
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"

	shared "github.com/totorialman/realtime-analytics-pipeline/shared"
)

type Generator struct {
	lastKey   []byte
	lastValue []byte
	mu        sync.Mutex
}

func NewGenerator() *Generator {
	return &Generator{}
}

func (g *Generator) Next() ProducedMessage {
	g.mu.Lock()
	defer g.mu.Unlock()

	if len(g.lastKey) > 0 && rand.Intn(100) == 0 {
		return ProducedMessage{
			Key:   append([]byte(nil), g.lastKey...),
			Value: append([]byte(nil), g.lastValue...),
		}
	}

	key := []byte("page-" + strconv.Itoa(rand.Intn(50)+1))
	event := generateEvent()

	if rand.Intn(100) < 5 {
		kind := rand.Intn(3)
		switch kind {
		case 0:
			event.PageID = ""
			payload, _ := event.ToJSON()
			g.lastKey = append([]byte(nil), key...)
			g.lastValue = append([]byte(nil), payload...)
			return ProducedMessage{Key: key, Value: payload}
		case 1:
			event.ViewDuration = -100
			payload, _ := event.ToJSON()
			g.lastKey = append([]byte(nil), key...)
			g.lastValue = append([]byte(nil), payload...)
			return ProducedMessage{Key: key, Value: payload}
		default:
			payload := []byte(`{"page_id":`)
			g.lastKey = append([]byte(nil), key...)
			g.lastValue = append([]byte(nil), payload...)
			return ProducedMessage{Key: key, Value: payload}
		}
	}

	payload, _ := event.ToJSON()
	g.lastKey = append([]byte(nil), key...)
	g.lastValue = append([]byte(nil), payload...)
	return ProducedMessage{Key: key, Value: payload}
}

func generateEvent() shared.PageViewEvent {
	isBounce := rand.Intn(100) < 10
	duration := 0
	if isBounce {
		duration = rand.Intn(4999) + 1
	} else {
		duration = rand.Intn(590001) + 10000
	}

	return shared.PageViewEvent{
		PageID:       "page-" + strconv.Itoa(rand.Intn(50)+1),
		UserID:       "user-" + strconv.Itoa(rand.Intn(2000)+1),
		ViewDuration: duration,
		Timestamp:    time.Now().UTC(),
		UserAgent:    randomUserAgent(),
		IPAddress:    randomIP(),
		Region:       randomRegion(),
		IsBounce:     isBounce,
	}
}

func randomUserAgent() string {
	values := []string{
		"Mozilla/5.0 Chrome/124.0",
		"Mozilla/5.0 Firefox/126.0",
		"Mozilla/5.0 Safari/17.0",
		"Mozilla/5.0 Edge/124.0",
	}
	return values[rand.Intn(len(values))]
}

func randomRegion() string {
	values := []string{
		"ru-central",
		"eu-west",
		"us-east",
		"ap-south",
	}
	return values[rand.Intn(len(values))]
}

func randomIP() string {
	ip := net.IPv4(byte(rand.Intn(223)+1), byte(rand.Intn(255)), byte(rand.Intn(255)), byte(rand.Intn(255)))
	return ip.String()
}