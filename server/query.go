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

// changeTypes is the allowed set for validation.
var changeTypes = map[string]bool{
	"deploy": true, "config": true, "rollback": true, "manual": true,
}

// NewQueryServer returns an http.Handler for the read API:
//
//	GET /v1/query/spans?trace_id=&name=&service=&from=&to=&limit=
//	GET /v1/query/metrics?name=&service=&from=&to=&limit=
//	GET /v1/query/logs?severity=&trace_id=&service=&from=&to=&limit=
//	GET /v1/query/anomalies?limit=
//	GET /v1/entities?type=service
//	GET /v1/topology
//	GET /v1/entity/signals?type=service&id=X
func NewQueryServer(store *storage.Storage, ew *ExperimentWorker, logger *zap.Logger) http.Handler {
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
	mux.HandleFunc("/v1/entity/span-logs", s.spanLogs)
	mux.HandleFunc("/v1/rca", s.rca)
	mux.HandleFunc("/v1/incidents", s.incidents)
	mux.HandleFunc("/v1/incidents/clear", s.clearIncidents)
	mux.HandleFunc("/v1/incident-groups", s.incidentGroups)
	mux.HandleFunc("/v1/changes", s.changes)
	// GenAI observability routes
	registerGenAIRoutes(mux, store, ew, logger)
	// Scenario simulation routes (remove registerScenarioRoutes + server/scenarios.go to disable)
	registerScenarioRoutes(mux, store, logger)
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
	rows, err := s.store.QueryAnomalies(r.Context(), r.URL.Query().Get("entity"), parseInt(r.URL.Query().Get("limit")))
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

	// Time window: default to last hour; callers can pass from/to (Unix ns) to
	// anchor the query around a specific anomaly timestamp.
	now := time.Now()
	fromNs := parseInt64(r.URL.Query().Get("from"))
	toNs := parseInt64(r.URL.Query().Get("to"))
	if fromNs == 0 {
		fromNs = now.Add(-time.Hour).UnixNano()
	}
	if toNs == 0 {
		toNs = now.UnixNano()
	}

	ctx := r.Context()
	k8sAttrs := entityK8sAttrs(ctx, s.store, entityID)

	spanRows, _ := s.store.QuerySpans(ctx, storage.SpanQuery{
		Service: entityID,
		From:    fromNs,
		To:      toNs,
		Limit:   100,
	})
	metricRows, _ := s.store.QueryMetrics(ctx, storage.MetricQuery{
		Service:  entityID,
		K8sAttrs: k8sAttrs,
		From:     fromNs,
		To:       toNs,
		Limit:    200,
	})
	logRows, _ := s.store.QueryLogs(ctx, storage.LogQuery{
		Service:  entityID,
		K8sAttrs: k8sAttrs,
		From:     fromNs,
		To:       toNs,
		Limit:    200,
	})
	anomalyRows, _ := s.store.QueryAnomalies(ctx, entityID, 50)

	// Surface ERROR spans first, then by recency.
	sort.SliceStable(spanRows, func(i, j int) bool {
		if spanRows[i].StatusCode != spanRows[j].StatusCode {
			return spanRows[i].StatusCode > spanRows[j].StatusCode // ERROR (2) before OK (1/0)
		}
		return spanRows[i].StartNs > spanRows[j].StartNs
	})

	// Surface ERROR/FATAL logs first, then by recency.
	sort.SliceStable(logRows, func(i, j int) bool {
		ri, rj := severityRank(logRows[i].Severity), severityRank(logRows[j].Severity)
		if ri != rj {
			return ri > rj
		}
		return logRows[i].TimestampNs > logRows[j].TimestampNs
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"entity_type": entityType,
		"entity_id":   entityID,
		"window":      map[string]int64{"from_ns": fromNs, "to_ns": toNs},
		"spans":       envelope{Data: spanRows, Count: len(spanRows)},
		"metrics":     envelope{Data: metricRows, Count: len(metricRows)},
		"logs":        envelope{Data: logRows, Count: len(logRows)},
		"anomalies":   envelope{Data: anomalyRows, Count: len(anomalyRows)},
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
	Resolved     bool           `json:"resolved"` // true when no new anomaly fired in last 5 min
}

// resolvedThresholdNs — incident is resolved if latest anomaly is older than this.
const resolvedThresholdNs = int64(30 * 60 * 1e9) // 30 minutes

// signalPriority maps signal types to priority scores for incident ranking.
var signalPriority = map[string]int{
	"missing_service":    6,
	"error_signature":    5,
	"correlated_incident": 5,
	"span_error_rate":    4,
	"trace_drift":        3,
	"callgraph_drift":    3,
	"span_latency":       2,
	"genai_latency_drift": 2,
	"metric":             1,
}

func (s *queryServer) incidents(w http.ResponseWriter, r *http.Request) {
	rows, err := s.store.QueryAnomalies(r.Context(), "", 2000)
	if err != nil {
		s.logger.Error("query anomalies for incidents", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 30-minute bucket in nanoseconds (DetectedAt is UnixNano).
	const bucketNs int64 = 30 * 60 * 1_000_000_000
	// Only consider anomalies from the last 60 minutes for active incident detection.
	windowNs := time.Now().UnixNano() - 60*60*1_000_000_000

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
		if row.EntityID == "" || row.DetectedAt < windowNs {
			continue
		}
		bucket := (row.DetectedAt / bucketNs) * bucketNs
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

	// By default suppress metric-only warning incidents (high-volume noise).
	// Pass ?all=true to include everything.
	importantOnly := r.URL.Query().Get("all") != "true"

	nowNs := time.Now().UnixNano()
	out := make([]IncidentOut, 0, len(inc))
	for k, d := range inc {
		// Skip metric-only incidents in default (important) mode regardless of severity.
		// Metric anomalies alone are not actionable enough to show by default.
		if importantOnly && d.priority <= 1 {
			continue
		}
		out = append(out, IncidentOut{
			EntityID:     k.entityID,
			BucketTs:     k.bucket,
			Signals:      d.signals,
			Priority:     d.priority,
			Severity:     d.severity,
			AnomalyCount: d.count,
			LatestTs:     d.latestTs,
			EarliestTs:   d.earliestTs,
			Resolved:     nowNs-d.latestTs > resolvedThresholdNs,
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
	case []storage.ChangeEventRow:
		count = len(v)
	}

	json.NewEncoder(w).Encode(envelope{Data: data, Count: count})
}

// spanLogs correlates logs to a specific span using the entity framework.
//
// Primary:  logs where trace_id matches the span's trace_id (exact, when OTel trace
//           context propagation is active in the logging framework).
// Fallback: logs where entity_id matches and timestamp falls within the span's time
//           window ±2 s (proximity match — works for any logging setup).
//
// GET /v1/entity/span-logs?entity=orders&span_start=<ns>&span_end=<ns>&trace_id=<id>
func (s *queryServer) spanLogs(w http.ResponseWriter, r *http.Request) {
	entityID := r.URL.Query().Get("entity")
	spanStart := parseInt64(r.URL.Query().Get("span_start"))
	spanEnd := parseInt64(r.URL.Query().Get("span_end"))
	traceID := r.URL.Query().Get("trace_id")

	if entityID == "" || spanStart == 0 {
		http.Error(w, "entity and span_start are required", http.StatusBadRequest)
		return
	}
	if spanEnd == 0 {
		spanEnd = spanStart
	}

	// Extend the window ±2 s to catch logs emitted just before/after the span.
	const bufNs = 2_000_000_000
	fromNs := spanStart - bufNs
	toNs := spanEnd + bufNs

	ctx := r.Context()
	logs, err := s.store.QueryLogs(ctx, storage.LogQuery{
		Service: entityID,
		From:    fromNs,
		To:      toNs,
		Limit:   100,
	})
	if err != nil {
		s.logger.Error("span-logs query", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Split into exact (trace_id match) and proximity buckets.
	// The zero-value OTel trace_id "00000000000000000000000000000000" is treated as absent.
	const zeroTraceID = "00000000000000000000000000000000"
	var exact, nearby []storage.LogRow
	for _, l := range logs {
		if traceID != "" && traceID != zeroTraceID && l.TraceID == traceID {
			exact = append(exact, l)
		} else {
			nearby = append(nearby, l)
		}
	}

	// Sort exact matches by severity then timestamp.
	sort.SliceStable(exact, func(i, j int) bool {
		ri, rj := severityRank(exact[i].Severity), severityRank(exact[j].Severity)
		if ri != rj {
			return ri > rj
		}
		return exact[i].TimestampNs < exact[j].TimestampNs
	})

	// Sort proximity matches by severity, then by closeness to span start.
	sort.SliceStable(nearby, func(i, j int) bool {
		ri, rj := severityRank(nearby[i].Severity), severityRank(nearby[j].Severity)
		if ri != rj {
			return ri > rj
		}
		di := absInt64(nearby[i].TimestampNs - spanStart)
		dj := absInt64(nearby[j].TimestampNs - spanStart)
		return di < dj
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"entity_id":     entityID,
		"span_start_ns": spanStart,
		"span_end_ns":   spanEnd,
		"trace_id":      traceID,
		"exact":         envelope{Data: exact, Count: len(exact)},
		"nearby":        envelope{Data: nearby, Count: len(nearby)},
	})
}

func parseInt(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}

func parseInt64(s string) int64 {
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}

// severityRank maps OTel/common log severity strings to a numeric rank for sorting.
func severityRank(sev string) int {
	switch sev {
	case "FATAL", "CRITICAL":
		return 4
	case "ERROR":
		return 3
	case "WARN", "WARNING":
		return 2
	default:
		return 1
	}
}

func absInt64(n int64) int64 {
	if n < 0 {
		return -n
	}
	return n
}

// changes handles POST (ingest) and GET (query) for change records.
//
//	POST /v1/changes  body: {"entity_id","change_type","description","author","link","timestamp"}
//	GET  /v1/changes?entity=&from=&to=&limit=
func (s *queryServer) changes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var e storage.ChangeEventRow
		if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		if e.EntityID == "" {
			http.Error(w, "entity_id is required", http.StatusBadRequest)
			return
		}
		if e.Description == "" {
			http.Error(w, "description is required", http.StatusBadRequest)
			return
		}
		if e.ChangeType != "" && !changeTypes[e.ChangeType] {
			http.Error(w, "change_type must be one of: deploy, config, rollback, manual", http.StatusBadRequest)
			return
		}
		id, err := s.store.InsertChangeEvent(r.Context(), e)
		if err != nil {
			s.logger.Error("insert change event", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"id": id, "status": "ok"})

	case http.MethodGet:
		entityID := r.URL.Query().Get("entity")
		fromSecs := parseInt64(r.URL.Query().Get("from"))
		toSecs := parseInt64(r.URL.Query().Get("to"))
		lim := parseInt(r.URL.Query().Get("limit"))
		rows, err := s.store.QueryChangeEvents(r.Context(), entityID, fromSecs, toSecs, lim)
		writeJSON(w, rows, err, s.logger)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *queryServer) clearIncidents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete && r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	cutoff := time.Now().UnixNano() - resolvedThresholdNs
	if err := s.store.ClearResolvedAnomalies(r.Context(), cutoff); err != nil {
		s.logger.Error("clear resolved anomalies", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *queryServer) incidentGroups(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	status := r.URL.Query().Get("status") // "active" | "resolved" | "" (all)
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	groups, err := s.store.QueryIncidentGroups(r.Context(), status, limit)
	if err != nil {
		s.logger.Error("query incident groups", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if groups == nil {
		groups = []storage.IncidentGroupRow{}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"groups": groups, "count": len(groups)})
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
