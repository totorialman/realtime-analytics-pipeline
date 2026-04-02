package handler

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/totorialman/realtime-analytics-pipeline/consumer/internal/domain"
)

func NewHandler(metrics *domain.Metrics) http.Handler {
	r := mux.NewRouter()

	r.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, metrics.Snapshot())
	}).Methods("GET")

	r.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}).Methods("GET")

	return r
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}