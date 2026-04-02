package handler

import (
	"encoding/json"
	stdhttp "net/http"

	"github.com/gorilla/mux"
	"github.com/totorialman/realtime-analytics-pipeline/consumer/internal/domain"
)

func NewHandler(metrics *domain.Metrics) stdhttp.Handler {
	r := mux.NewRouter()

	r.HandleFunc("/metrics", func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		writeJSON(w, metrics.Snapshot())
	}).Methods("GET")

	r.HandleFunc("/healthz", func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
		w.WriteHeader(stdhttp.StatusOK)
	}).Methods("GET")

	return r
}

func writeJSON(w stdhttp.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}