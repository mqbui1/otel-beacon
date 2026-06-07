package server

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
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
	mux.HandleFunc("/v1/environments", s.environments)
	mux.HandleFunc("/v1/topology", s.topology)
	mux.HandleFunc("/v1/entity/signals", s.entitySignals)
	mux.HandleFunc("/v1/rca", s.rca)
	mux.HandleFunc("/v1/incidents", s.incidents)
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
		Traces:  parseInt(r.URL.Query().Get("traces")),
		Service: r.URL.Query().Get("service"),
		From:    parseInt64(r.URL.Query().Get("from")),
		To:      parseInt64(r.URL.Query().Get("to")),
		Limit:   parseInt(r.URL.Query().Get("limit")),
	}
	rows, err := s.store.QuerySpans(r.Context(), q)
	writeJSON(w, rows, err, s.logger)
}

func (s *queryServer) metrics(w http.ResponseWriter, r *http.Request) {
	service := r.URL.Query().Get("service")
	var namePrefixes []string
	if raw := r.URL.Query().Get("prefix"); raw != "" {
		for _, p := range strings.Split(raw, ",") {
			if p = strings.TrimSpace(p); p != "" {
				namePrefixes = append(namePrefixes, p)
			}
		}
	}
	q := storage.MetricQuery{
		Name:         r.URL.Query().Get("name"),
		NamePrefixes: namePrefixes,
		Service:      service,
		K8sAttrs:     entityK8sAttrs(r.Context(), s.store, service),
		From:         parseInt64(r.URL.Query().Get("from")),
		To:           parseInt64(r.URL.Query().Get("to")),
		Limit:        parseInt(r.URL.Query().Get("limit")),
	}
	rows, err := s.store.QueryMetrics(r.Context(), q)
	writeJSON(w, rows, err, s.logger)
}

func (s *queryServer) logs(w http.ResponseWriter, r *http.Request) {
	service := r.URL.Query().Get("service")
	q := storage.LogQuery{
		Severity: r.URL.Query().Get("severity"),
		TraceID:  r.URL.Query().Get("trace_id"),
		Service:  service,
		K8sAttrs: entityK8sAttrs(r.Context(), s.store, service),
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
	rows, err := s.store.QueryEntities(r.Context(), r.URL.Query().Get("type"), r.URL.Query().Get("env"))
	writeJSON(w, rows, err, s.logger)
}

func (s *queryServer) environments(w http.ResponseWriter, r *http.Request) {
	envs, err := s.store.QueryEnvironments(r.Context())
	if err != nil {
		s.logger.Error("query environments", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"environments": envs})
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
		Limit:   200,
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

// IncidentOut groups anomalies by (entity_id, 30-min bucket) into actionable incidents.
type IncidentOut struct {
	EntityID     string         `json:"entity_id"`
	BucketTs     int64          `json:"bucket_ts"`    // Unix seconds, start of 30-min window
	Signals      map[string]int `json:"signals"`      // signal_type → count
	Priority     int            `json:"priority"`     // highest signal priority in incident
	Severity     string         `json:"severity"`     // "critical" if any, else "warning"
	AnomalyCount int            `json:"anomaly_count"`
	LatestTs     int64          `json:"latest_ts"`
	EarliestTs   int64          `json:"earliest_ts"`
}

// signalPriority maps signal types to priority scores for incident ranking.
var signalPriority = map[string]int{
	"error_signature": 5,
	"span_error_rate": 4,
	"trace_drift":     3,
	"span_latency":    2,
	"metric":          1,
}

func (s *queryServer) incidents(w http.ResponseWriter, r *http.Request) {
	rows, err := s.store.QueryAnomalies(r.Context(), 2000)
	if err != nil {
		s.logger.Error("query anomalies for incidents", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	const bucketSecs int64 = 30 * 60

	type incKey struct {
		entityID string
		bucket   int64
	}
	type incData struct {
		signals    map[string]int
		priority   int
		severity   string
		count      int
		latestTs   int64
		earliestTs int64
	}

	inc := map[incKey]*incData{}
	for _, row := range rows {
		if row.EntityID == "" {
			continue
		}
		bucket := (row.DetectedAt / bucketSecs) * bucketSecs
		k := incKey{row.EntityID, bucket}
		d, ok := inc[k]
		if !ok {
			d = &incData{
				signals:    map[string]int{},
				severity:   "warning",
				earliestTs: row.DetectedAt,
			}
			inc[k] = d
		}
		d.signals[row.SignalType]++
		d.count++
		if row.DetectedAt > d.latestTs {
			d.latestTs = row.DetectedAt
		}
		if row.DetectedAt < d.earliestTs {
			d.earliestTs = row.DetectedAt
		}
		if p := signalPriority[row.SignalType]; p > d.priority {
			d.priority = p
		}
		if row.Severity == "critical" {
			d.severity = "critical"
		}
	}

	out := make([]IncidentOut, 0, len(inc))
	for k, d := range inc {
		out = append(out, IncidentOut{
			EntityID:     k.entityID,
			BucketTs:     k.bucket,
			Signals:      d.signals,
			Priority:     d.priority,
			Severity:     d.severity,
			AnomalyCount: d.count,
			LatestTs:     d.latestTs,
			EarliestTs:   d.earliestTs,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority > out[j].Priority
		}
		return out[i].LatestTs > out[j].LatestTs
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(envelope{Data: out, Count: len(out)})
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

// entityK8sAttrs looks up a service entity and returns its k8s resource attributes
// (pod, deployment, namespace, node) so metrics/logs can be correlated even when
// the emitting agent does not inject service.name.
func entityK8sAttrs(ctx context.Context, store *storage.Storage, service string) map[string]string {
	if service == "" {
		return nil
	}
	entities, err := store.QueryEntities(ctx, "service", "")
	if err != nil {
		return nil
	}
	for _, e := range entities {
		if e.EntityID != service {
			continue
		}
		var attrs map[string]any
		if json.Unmarshal([]byte(e.Attrs), &attrs) != nil {
			return nil
		}
		k8s := map[string]string{}
		for _, key := range []string{
			"k8s.pod.name",
			"k8s.deployment.name",
			// namespace and node are too broad — they match all pods on that node/namespace
		} {
			if v, ok := attrs[key].(string); ok && v != "" {
				k8s[key] = v
			}
		}
		if len(k8s) > 0 {
			return k8s
		}
		return nil
	}
	return nil
}
