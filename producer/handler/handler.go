package handler

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/totorialman/realtime-analytics-pipeline/producer/internal/domain"
	"github.com/totorialman/realtime-analytics-pipeline/producer/internal/service"
	shared "github.com/totorialman/realtime-analytics-pipeline/shared"
)

type UpdateRequest struct {
	GenerationMode    shared.GenerationMode    `json:"generation_mode"`
	SendMode          shared.SendMode          `json:"send_mode"`
	PartitionStrategy shared.PartitionStrategy `json:"partition_strategy"`
	RegularRate       int                      `json:"regular_rate"`
}

func NewHandler(service *service.Service, metrics *domain.Metrics) http.Handler {
	r := mux.NewRouter()

	r.HandleFunc("/rate", func(w http.ResponseWriter, r *http.Request) {
		var req UpdateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		service.Update(req.GenerationMode, req.SendMode, req.PartitionStrategy, req.RegularRate)
		writeJSON(w, service.Snapshot())
	}).Methods("POST")

	r.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"sent_total":       metrics.SentTotal.Load(),
			"errors_total":     metrics.ErrorTotal.Load(),
			"duplicates_total": metrics.DuplicateTotal.Load(),
			"avg_latency_ms":   metrics.AverageLatencyMs(),
		})
	}).Methods("GET")

	r.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}).Methods("GET")

	return r
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
