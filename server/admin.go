package server

import (
	"encoding/json"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewAdminServer returns an http.Handler serving:
//
//	GET /healthz  — liveness (always 200 while process is running)
//	GET /readyz   — readiness (200 when ping returns nil)
//	GET /metrics  — Prometheus metrics
//
// Pass ping=nil to skip the DB check on /readyz (always returns ready).
func NewAdminServer(ping func() error) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if ping != nil {
			if err := ping(); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				json.NewEncoder(w).Encode(map[string]string{"status": "unavailable", "error": err.Error()})
				return
			}
		}
		w.Write([]byte(`{"status":"ready"}`))
	})
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}
