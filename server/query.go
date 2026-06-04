package server

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/yourorg/otel-backend/storage"
)

// NewQueryServer returns an http.Handler for the read API:
//
//	GET /v1/query/spans?trace_id=&name=&service=&from=&to=&limit=
//	GET /v1/query/metrics?name=&service=&from=&to=&limit=
//	GET /v1/query/logs?severity=&trace_id=&service=&from=&to=&limit=
//	GET /v1/query/anomalies?limit=
//	GET /v1/entities?type=service
//	GET /v1/topology
//	GET /v1/entity/signals?type=service&id=X
func NewQueryServer(store *storage.Storage, logger *zap.Logger) http.Handler {
	mux := http.NewServeMux()
	s := &queryServer{store: store, logger: logger}
	mux.HandleFunc("/v1/query/spans", s.spans)
	mux.HandleFunc("/v1/query/metrics", s.metrics)
	mux.HandleFunc("/v1/query/logs", s.logs)
	mux.HandleFunc("/v1/query/anomalies", s.anomalies)
	mux.HandleFunc("/v1/entities", s.entities)
	mux.HandleFunc("/v1/topology", s.topology)
	mux.HandleFunc("/v1/entity/signals", s.entitySignals)
	return mux
}

type queryServer struct {
	store  *storage.Storage
	logger *zap.Logger
}

func (s *queryServer) spans(w http.ResponseWriter, r *http.Request) {
	q := storage.SpanQuery{
		TraceID: r.URL.Query().Get("trace_id"),
		Name:    r.URL.Query().Get("name"),
		Service: r.URL.Query().Get("service"),
		From:    parseInt64(r.URL.Query().Get("from")),
		To:      parseInt64(r.URL.Query().Get("to")),
		Limit:   parseInt(r.URL.Query().Get("limit")),
	}
	rows, err := s.store.QuerySpans(r.Context(), q)
	writeJSON(w, rows, err, s.logger)
}

func (s *queryServer) metrics(w http.ResponseWriter, r *http.Request) {
	q := storage.MetricQuery{
		Name:    r.URL.Query().Get("name"),
		Service: r.URL.Query().Get("service"),
		From:    parseInt64(r.URL.Query().Get("from")),
		To:      parseInt64(r.URL.Query().Get("to")),
		Limit:   parseInt(r.URL.Query().Get("limit")),
	}
	rows, err := s.store.QueryMetrics(r.Context(), q)
	writeJSON(w, rows, err, s.logger)
}

func (s *queryServer) logs(w http.ResponseWriter, r *http.Request) {
	q := storage.LogQuery{
		Severity: r.URL.Query().Get("severity"),
		TraceID:  r.URL.Query().Get("trace_id"),
		Service:  r.URL.Query().Get("service"),
		From:     parseInt64(r.URL.Query().Get("from")),
		To:       parseInt64(r.URL.Query().Get("to")),
		Limit:    parseInt(r.URL.Query().Get("limit")),
	}
	rows, err := s.store.QueryLogs(r.Context(), q)
	writeJSON(w, rows, err, s.logger)
}

func (s *queryServer) anomalies(w http.ResponseWriter, r *http.Request) {
	rows, err := s.store.QueryAnomalies(r.Context(), parseInt(r.URL.Query().Get("limit")))
	writeJSON(w, rows, err, s.logger)
}

func (s *queryServer) entities(w http.ResponseWriter, r *http.Request) {
	rows, err := s.store.QueryEntities(r.Context(), r.URL.Query().Get("type"))
	writeJSON(w, rows, err, s.logger)
}

func (s *queryServer) topology(w http.ResponseWriter, r *http.Request) {
	rows, err := s.store.QueryTopology(r.Context())
	writeJSON(w, rows, err, s.logger)
}

func (s *queryServer) entitySignals(w http.ResponseWriter, r *http.Request) {
	entityType := r.URL.Query().Get("type")
	entityID := r.URL.Query().Get("id")
	if entityType == "" || entityID == "" {
		http.Error(w, "type and id are required", http.StatusBadRequest)
		return
	}

	fromNs := time.Now().Add(-time.Hour).UnixNano()
	ctx := r.Context()

	spanRows, spanErr := s.store.QuerySpans(ctx, storage.SpanQuery{
		Service: entityID,
		From:    fromNs,
		Limit:   50,
	})
	metricRows, metricErr := s.store.QueryMetrics(ctx, storage.MetricQuery{
		Service: entityID,
		From:    fromNs,
		Limit:   50,
	})
	logRows, logErr := s.store.QueryLogs(ctx, storage.LogQuery{
		Service: entityID,
		From:    fromNs,
		Limit:   50,
	})

	for _, err := range []error{spanErr, metricErr, logErr} {
		if err != nil {
			s.logger.Error("entity signals query", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"entity_type": entityType,
		"entity_id":   entityID,
		"spans":       envelope{Data: spanRows, Count: len(spanRows)},
		"metrics":     envelope{Data: metricRows, Count: len(metricRows)},
		"logs":        envelope{Data: logRows, Count: len(logRows)},
	})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type envelope struct {
	Data  any `json:"data"`
	Count int `json:"count"`
}

func writeJSON(w http.ResponseWriter, data any, err error, logger *zap.Logger) {
	if err != nil {
		logger.Error("query error", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	// Reflect the length if data is a slice. Use a type switch for the known types.
	count := 0
	switch v := data.(type) {
	case []storage.SpanRow:
		count = len(v)
	case []storage.MetricRow:
		count = len(v)
	case []storage.LogRow:
		count = len(v)
	case []storage.AnomalyRow:
		count = len(v)
	case []storage.EntityRow:
		count = len(v)
	case []storage.TopologyEdge:
		count = len(v)
	}

	json.NewEncoder(w).Encode(envelope{Data: data, Count: count})
}

func parseInt(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

func parseInt64(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}
