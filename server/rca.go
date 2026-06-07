package server

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/yourorg/otel-backend/storage"
)

const defaultWindowSecs = 300

// ─── Result types ─────────────────────────────────────────────────────────────

type EntityHealth struct {
	EntityID    string  `json:"entity_id"`
	SpanTotal   int     `json:"span_total"`
	SpanErrors  int     `json:"span_errors"`
	ErrorRate   float64 `json:"error_rate"`
	AvgMs       float64 `json:"avg_ms"`
	P95Ms       float64 `json:"p95_ms"`
	LogErrors   int     `json:"log_errors"`
	LogWarns    int     `json:"log_warns"`
	CpuUsage    float64 `json:"cpu_usage"`    // 0..1 (cores)
	MemRssBytes int64   `json:"mem_rss_bytes"` // bytes
	HasData     bool    `json:"has_data"`
}

type NeighborHealth struct {
	EntityID   string       `json:"entity_id"`
	Relation   string       `json:"relation"`    // "upstream" | "downstream" | "co_located"
	Health     EntityHealth `json:"health"`
	LagSeconds float64      `json:"lag_seconds"` // negative = degraded before focal
}

type CauseCandidate struct {
	EntityID   string  `json:"entity_id"`
	CauseType  string  `json:"cause_type"` // "downstream_error" | "infra_pressure" | "upstream_spike"
	Confidence float64 `json:"confidence"` // 0..1
	Evidence   string  `json:"evidence"`
}

type RCAResult struct {
	Entity             string                        `json:"entity"`
	IncidentTs         int64                         `json:"incident_ts"`
	WindowSecs         int                           `json:"window_secs"`
	Health             EntityHealth                  `json:"health"`
	Baseline           EntityHealth                  `json:"baseline"` // health in previous window (before incident)
	Upstream           []NeighborHealth              `json:"upstream"`
	Downstream         []NeighborHealth              `json:"downstream"`
	CoLocated          []NeighborHealth              `json:"co_located"`
	CandidateCauses    []CauseCandidate              `json:"candidate_causes"`
	ErrorSignatures    []storage.ErrorSignatureRow   `json:"error_signatures,omitempty"`
	TraceFingerprints  []storage.TraceFingerprintRow `json:"trace_fingerprints,omitempty"`
	Narrative          string                        `json:"narrative,omitempty"`
}

// ─── HTTP handler ─────────────────────────────────────────────────────────────

func (s *queryServer) rca(w http.ResponseWriter, r *http.Request) {
	entityID := r.URL.Query().Get("entity")
	if entityID == "" {
		http.Error(w, "entity is required", http.StatusBadRequest)
		return
	}

	var incidentTs int64
	if raw := r.URL.Query().Get("ts"); raw != "" {
		if v, err := strconv.ParseInt(raw, 10, 64); err == nil && v > 0 {
			// ts is Unix seconds (from anomaly detected_at); convert to nanoseconds
			if v < 1e12 {
				incidentTs = v * 1_000_000_000
			} else {
				incidentTs = v // already nanoseconds
			}
		}
	}
	if incidentTs == 0 {
		incidentTs = time.Now().UnixNano()
	}

	windowSecs := defaultWindowSecs
	if raw := r.URL.Query().Get("window"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			windowSecs = n
		}
	}

	withAI := r.URL.Query().Get("ai") == "true"
	ctx := r.Context()
	windowNs := int64(windowSecs) * 1_000_000_000
	fromNs := incidentTs - windowNs
	toNs := incidentTs

	// Load all service entities once — avoids repeated DB calls in co-location checks
	allEntities, _ := s.store.QueryEntities(ctx, "service", "")
	attrsMap := buildEntityAttrsMap(allEntities)

	// k8s filter attrs for the focal entity (pod + deployment only — no node/namespace)
	k8sFilter := filterAttrs(attrsMap[entityID])

	// Focal entity: incident window + baseline (previous window)
	health := s.entityHealth(ctx, entityID, k8sFilter, fromNs, toNs)
	baseline := s.entityHealth(ctx, entityID, k8sFilter, fromNs-windowNs, fromNs)

	// Topology
	edges, _ := s.store.QueryTopology(ctx)
	upstreamIDs, downstreamIDs := topologyNeighbors(entityID, edges)

	// Co-located services (same k8s.node.name as focal entity)
	nodeName := attrsMap[entityID]["k8s.node.name"]
	coLocatedIDs := coLocatedServices(entityID, nodeName, attrsMap)

	// Neighbor health with pre-window for lag detection
	upstream := s.neighborHealthList(ctx, upstreamIDs, "upstream", attrsMap, fromNs, toNs, windowNs)
	downstream := s.neighborHealthList(ctx, downstreamIDs, "downstream", attrsMap, fromNs, toNs, windowNs)
	coLocated := s.neighborHealthList(ctx, coLocatedIDs, "co_located", attrsMap, fromNs, toNs, windowNs)

	candidates := rankCauses(health, baseline, upstream, downstream, coLocated)

	// Error signatures: new / spiking error patterns for the focal entity
	errorSigs, _ := s.store.QueryErrorSignatures(ctx, entityID)
	for _, sig := range errorSigs {
		if sig.IsBaseline {
			continue // known pattern, not novel
		}
		conf := math.Min(0.9, 0.6+math.Min(0.3, float64(sig.OccurrenceCount)/10))
		errType := sig.ErrorType
		if errType == "" {
			errType = "unknown"
		}
		candidates = append(candidates, CauseCandidate{
			EntityID:  entityID,
			CauseType: "error_signature",
			Confidence: conf,
			Evidence: fmt.Sprintf("new error pattern: type=%s op=%s (seen %d times, first at %s)",
				errType, sig.Operation, sig.OccurrenceCount,
				time.Unix(sig.FirstSeenAt/1_000_000_000, 0).Local().Format("15:04:05")),
		})
	}

	// Trace fingerprints: novel call paths not seen in baseline
	traceFingerprints, _ := s.store.QueryTraceFingerprints(ctx, entityID)
	for _, fp := range traceFingerprints {
		if fp.IsBaseline {
			continue // known structure, not novel
		}
		conf := math.Min(0.85, 0.55+math.Min(0.3, float64(fp.OccurrenceCount)/5))
		candidates = append(candidates, CauseCandidate{
			EntityID:  entityID,
			CauseType: "trace_drift",
			Confidence: conf,
			Evidence: fmt.Sprintf("new call path rooted at %s: %d edge(s), seen %d time(s) (first at %s)",
				fp.RootService, edgeCount(fp.EdgeList), fp.OccurrenceCount,
				time.Unix(fp.FirstSeenAt/1_000_000_000, 0).Local().Format("15:04:05")),
		})
	}

	sort.Slice(candidates, func(i, j int) bool { return candidates[i].Confidence > candidates[j].Confidence })

	result := RCAResult{
		Entity:             entityID,
		IncidentTs:         incidentTs,
		WindowSecs:         windowSecs,
		Health:             health,
		Baseline:           baseline,
		Upstream:           upstream,
		Downstream:         downstream,
		CoLocated:          coLocated,
		CandidateCauses:    candidates,
		ErrorSignatures:    errorSigs,
		TraceFingerprints:  traceFingerprints,
	}

	if withAI {
		narrative, err := generateNarrative(ctx, result)
		if err != nil {
			s.logger.Warn("bedrock narrative failed", zap.Error(err))
			result.Narrative = "[AI summary unavailable: " + err.Error() + "]"
		} else {
			result.Narrative = narrative
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// ─── Health computation ───────────────────────────────────────────────────────

func (s *queryServer) entityHealth(ctx context.Context, entityID string, k8sAttrs map[string]string, fromNs, toNs int64) EntityHealth {
	h := EntityHealth{EntityID: entityID}

	spans, _ := s.store.QuerySpans(ctx, storage.SpanQuery{
		Service: entityID,
		From:    fromNs,
		To:      toNs,
		Limit:   2000,
	})
	if len(spans) > 0 {
		h.HasData = true
		h.SpanTotal = len(spans)
		durations := make([]float64, 0, len(spans))
		for _, sp := range spans {
			if sp.StatusCode == 2 {
				h.SpanErrors++
			}
			h.AvgMs += sp.DurationMs
			durations = append(durations, sp.DurationMs)
		}
		h.AvgMs /= float64(h.SpanTotal)
		h.ErrorRate = float64(h.SpanErrors) / float64(h.SpanTotal)
		sort.Float64s(durations)
		h.P95Ms = p95(durations)
	}

	logs, _ := s.store.QueryLogs(ctx, storage.LogQuery{
		Service:  entityID,
		K8sAttrs: k8sAttrs,
		From:     fromNs,
		To:       toNs,
		Limit:    500,
	})
	for _, l := range logs {
		switch l.Severity {
		case "ERROR", "FATAL", "CRITICAL":
			h.LogErrors++
		case "WARN", "WARNING":
			h.LogWarns++
		}
	}

	metrics, _ := s.store.QueryMetrics(ctx, storage.MetricQuery{
		Service:  entityID,
		K8sAttrs: k8sAttrs,
		From:     fromNs,
		To:       toNs,
		Limit:    200,
	})
	for _, m := range metrics {
		switch m.Name {
		case "container.cpu.usage":
			if m.Value > h.CpuUsage {
				h.CpuUsage = m.Value
				h.HasData = true
			}
		case "container.memory.rss":
			if int64(m.Value) > h.MemRssBytes {
				h.MemRssBytes = int64(m.Value)
				h.HasData = true
			}
		}
	}
	return h
}

func (s *queryServer) neighborHealthList(
	ctx context.Context,
	ids []string,
	relation string,
	attrsMap map[string]map[string]string,
	fromNs, toNs, windowNs int64,
) []NeighborHealth {
	out := make([]NeighborHealth, 0, len(ids))
	for _, id := range ids {
		k8s := filterAttrs(attrsMap[id])
		h := s.entityHealth(ctx, id, k8s, fromNs, toNs)
		pre := s.entityHealth(ctx, id, k8s, fromNs-windowNs, fromNs)

		// Negative lag: neighbor was already degraded in the pre-window
		lag := 0.0
		if pre.HasData && pre.ErrorRate > 0.1 {
			lag = -float64(windowNs) / 1_000_000_000
		}

		out = append(out, NeighborHealth{
			EntityID:   id,
			Relation:   relation,
			Health:     h,
			LagSeconds: lag,
		})
	}
	return out
}

// ─── Topology + co-location helpers ──────────────────────────────────────────

func topologyNeighbors(entityID string, edges []storage.TopologyEdge) (upstream, downstream []string) {
	seen := map[string]bool{}
	for _, e := range edges {
		if e.TargetService == entityID && !seen[e.SourceService] {
			upstream = append(upstream, e.SourceService)
			seen[e.SourceService] = true
		}
		if e.SourceService == entityID && !seen[e.TargetService] {
			downstream = append(downstream, e.TargetService)
			seen[e.TargetService] = true
		}
	}
	return
}

func coLocatedServices(entityID, nodeName string, attrsMap map[string]map[string]string) []string {
	if nodeName == "" {
		return nil
	}
	var out []string
	for id, attrs := range attrsMap {
		if id != entityID && attrs["k8s.node.name"] == nodeName {
			out = append(out, id)
		}
	}
	return out
}

// buildEntityAttrsMap parses all entity resource attrs into map[entityID → map[attr → value]].
// Includes k8s.node.name (used for co-location), which is intentionally excluded from query filters.
func buildEntityAttrsMap(entities []storage.EntityRow) map[string]map[string]string {
	m := make(map[string]map[string]string, len(entities))
	for _, e := range entities {
		var raw map[string]any
		if err := json.Unmarshal([]byte(e.Attrs), &raw); err != nil {
			continue
		}
		attrs := make(map[string]string)
		for _, k := range []string{
			"k8s.pod.name", "k8s.deployment.name", "k8s.namespace.name",
			"k8s.node.name", "k8s.container.name",
		} {
			if v, ok := raw[k].(string); ok && v != "" {
				attrs[k] = v
			}
		}
		m[e.EntityID] = attrs
	}
	return m
}

// filterAttrs returns only pod+deployment attrs — safe to use as metric/log query filters.
func filterAttrs(attrs map[string]string) map[string]string {
	if attrs == nil {
		return nil
	}
	out := make(map[string]string)
	for _, k := range []string{"k8s.pod.name", "k8s.deployment.name"} {
		if v := attrs[k]; v != "" {
			out[k] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// ─── Cause ranking ────────────────────────────────────────────────────────────

func rankCauses(focal, baseline EntityHealth, upstream, downstream, coLocated []NeighborHealth) []CauseCandidate {
	var out []CauseCandidate

	for _, n := range downstream {
		if !n.Health.HasData || n.Health.ErrorRate < 0.05 {
			continue
		}
		conf := math.Min(0.95, 0.4+n.Health.ErrorRate)
		if n.LagSeconds < 0 {
			conf = math.Min(0.95, conf+0.25)
		}
		out = append(out, CauseCandidate{
			EntityID:   n.EntityID,
			CauseType:  "downstream_error",
			Confidence: conf,
			Evidence:   fmt.Sprintf("%s error rate %.0f%% (P95 %.1fms)%s", n.EntityID, n.Health.ErrorRate*100, n.Health.P95Ms, lagNote(n.LagSeconds)),
		})
	}

	for _, n := range coLocated {
		if !n.Health.HasData || n.Health.CpuUsage < 0.5 {
			continue
		}
		conf := math.Min(0.8, 0.3+n.Health.CpuUsage*0.5)
		if n.LagSeconds < 0 {
			conf = math.Min(0.8, conf+0.15)
		}
		out = append(out, CauseCandidate{
			EntityID:   n.EntityID,
			CauseType:  "infra_pressure",
			Confidence: conf,
			Evidence:   fmt.Sprintf("co-located %s CPU %.0f%%%s", n.EntityID, n.Health.CpuUsage*100, lagNote(n.LagSeconds)),
		})
	}

	for _, n := range upstream {
		if !n.Health.HasData || focal.SpanTotal == 0 {
			continue
		}
		if n.Health.SpanTotal > focal.SpanTotal*2 {
			out = append(out, CauseCandidate{
				EntityID:   n.EntityID,
				CauseType:  "upstream_spike",
				Confidence: 0.35,
				Evidence:   fmt.Sprintf("%s traffic %dx focal (%d vs %d spans)", n.EntityID, n.Health.SpanTotal/focal.SpanTotal, n.Health.SpanTotal, focal.SpanTotal),
			})
		}
	}

	// Self: focal entity error rate is itself the problem
	if focal.HasData && focal.ErrorRate > 0.05 {
		conf := math.Min(0.8, 0.4+focal.ErrorRate)
		if baseline.HasData && baseline.ErrorRate > 0 && focal.ErrorRate > baseline.ErrorRate*2 {
			conf = math.Min(0.9, conf+0.15) // regression vs baseline
		}
		out = append(out, CauseCandidate{
			EntityID:   focal.EntityID,
			CauseType:  "self_error",
			Confidence: conf,
			Evidence:   fmt.Sprintf("focal error rate %.0f%% (P95 %.1fms, %d spans)", focal.ErrorRate*100, focal.P95Ms, focal.SpanTotal),
		})
	}

	// Self: focal entity has high CPU pressure
	if focal.HasData && focal.CpuUsage > 0.7 {
		conf := math.Min(0.75, 0.3+focal.CpuUsage*0.4)
		out = append(out, CauseCandidate{
			EntityID:   focal.EntityID,
			CauseType:  "self_infra",
			Confidence: conf,
			Evidence:   fmt.Sprintf("focal CPU %.0f%%%s", focal.CpuUsage*100, func() string {
				if focal.MemRssBytes > 0 {
					return fmt.Sprintf(", mem %dMB", focal.MemRssBytes/1_000_000)
				}
				return ""
			}()),
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Confidence > out[j].Confidence })
	return out
}

func lagNote(lagSecs float64) string {
	if lagSecs < -30 {
		return fmt.Sprintf(" [degraded %.0fs before window]", -lagSecs)
	}
	return ""
}

// edgeCount returns the number of edges in a JSON-encoded edge list string.
func edgeCount(edgeListJSON string) int {
	var edges []string
	if json.Unmarshal([]byte(edgeListJSON), &edges) != nil {
		return 0
	}
	return len(edges)
}

func p95(sorted []float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := 0.95 * float64(len(sorted)-1)
	lo := int(math.Floor(idx))
	hi := int(math.Ceil(idx))
	if lo == hi {
		return sorted[lo]
	}
	return sorted[lo] + (sorted[hi]-sorted[lo])*(idx-float64(lo))
}
