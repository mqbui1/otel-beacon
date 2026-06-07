package storage

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	fpWindowDur      = 5 * time.Minute
	fpBaselineMinOcc = 3               // occurrences before promoting to baseline
	fpBaselineMinAge = 5 * time.Minute // must be this old before promoting
	errSpikeMultiple = 3.0             // rate > N× baseline = spike anomaly
	errBaselineMinOcc = 2
	errBaselineMinAge = 30 * time.Minute

	missingCheckInterval   = 5 * time.Minute
	missingAlertThreshold  = 2 * missingCheckInterval // silence this long before alerting
)

// noisePatterns are root operation substrings to exclude from fingerprinting.
// These are health checks, readiness probes, and infrastructure heartbeats that
// generate spurious "new fingerprint" alerts and clutter the baseline.
var noisePatterns = []string{
	"/health", "/healthz", "/readyz", "/livez", "/ready", "/live",
	"/actuator", "/ping", "/status", "/_health", "/api/health",
	"/metrics", "/favicon",
	"/eureka/", "/v1/agent/", "/v1/health/", "/v1/catalog/",
}

func isNoiseOperation(op string) bool {
	lower := strings.ToLower(op)
	for _, p := range noisePatterns {
		if strings.Contains(lower, p) {
			return true
		}
	}
	return false
}

// ── Trace fingerprint worker ─────────────────────────────────────────────────

func (s *Storage) fingerprintWorker() {
	defer s.wg.Done()
	// Wait for initial data to accumulate
	select {
	case <-time.After(2 * time.Minute):
	case <-s.ctx.Done():
		return
	}
	ticker := time.NewTicker(fpWindowDur)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := s.runTraceFingerprint(); err != nil {
				s.onError(fmt.Errorf("trace fingerprint: %w", err))
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Storage) runTraceFingerprint() error {
	ctx := s.ctx
	now := time.Now()
	toNs := now.UnixNano()
	fromNs := now.Add(-fpWindowDur).UnixNano()

	spans, err := s.backend.QuerySpans(ctx, SpanQuery{
		From: fromNs, To: toNs, InternalLimit: 5000,
	})
	if err != nil {
		return err
	}

	// Group by trace_id
	byTrace := make(map[string][]SpanRow)
	for _, sp := range spans {
		byTrace[sp.TraceID] = append(byTrace[sp.TraceID], sp)
	}

	// Load existing fingerprints (all services)
	existing, err := s.backend.QueryTraceFingerprints(ctx, "")
	if err != nil {
		return err
	}
	baseline := make(map[string]TraceFingerprintRow)
	candidates := make(map[string]TraceFingerprintRow)
	for _, fp := range existing {
		if fp.IsBaseline {
			baseline[fp.Hash] = fp
		} else {
			candidates[fp.Hash] = fp
		}
	}

	// Bootstrap mode: if no baseline exists yet, silently promote everything
	// to baseline on first pass rather than flooding with false-positive alerts.
	bootstrapMode := len(baseline) == 0

	var anomalies []AnomalyRow
	seen := make(map[string]bool)
	ts := now.Unix()

	for _, traceSpans := range byTrace {
		if len(traceSpans) < 2 {
			continue
		}
		hash, edges, rootSvc, rootOp := buildTraceFP(traceSpans)
		if hash == "" || len(edges) == 0 || seen[hash] {
			continue
		}
		// Filter out health checks, readiness probes, and infra heartbeats
		if isNoiseOperation(rootOp) {
			continue
		}
		seen[hash] = true

		// Always update last-seen for the root service so the missing-service
		// checker knows traffic is still flowing, even for baseline traces.
		if rootSvc != "" {
			s.missingMu.Lock()
			s.lastSeenRootSvc[rootSvc] = time.Now()
			delete(s.missingEmitted, rootSvc) // reset alert gate when traffic resumes
			s.missingMu.Unlock()
		}

		edgesJSON, _ := json.Marshal(edges)

		if fp, inBaseline := baseline[hash]; inBaseline {
			fp.LastSeenAt = ts
			fp.OccurrenceCount++
			_ = s.backend.UpsertTraceFingerprint(ctx, fp)
			continue
		}

		cand, isCandidate := candidates[hash]
		if !isCandidate {
			cand = TraceFingerprintRow{
				Hash:            hash,
				RootService:     rootSvc,
				EdgeList:        string(edgesJSON),
				OccurrenceCount: 1,
				FirstSeenAt:     ts,
				LastSeenAt:      ts,
			}
		} else {
			cand.OccurrenceCount++
			cand.LastSeenAt = ts
		}

		// In bootstrap mode (empty baseline on first run), silently promote
		// every unique fingerprint to baseline without alerting.
		if bootstrapMode {
			cand.IsBaseline = true
			_ = s.backend.UpsertTraceFingerprint(ctx, cand)
			continue
		}

		age := time.Duration(ts-cand.FirstSeenAt) * time.Second
		if cand.OccurrenceCount >= fpBaselineMinOcc && age >= fpBaselineMinAge {
			cand.IsBaseline = true
			_ = s.backend.UpsertTraceFingerprint(ctx, cand)
			continue
		}
		_ = s.backend.UpsertTraceFingerprint(ctx, cand)

		// Only emit anomaly on very first detection (not on subsequent windows)
		if !isCandidate {
			anomalies = append(anomalies, AnomalyRow{
				EntityID:     rootSvc,
				SignalType:   "trace_drift",
				DetectorName: "Trace Structural Drift",
				MetricName:   "trace.fingerprint",
				Value:        1,
				Score:        1,
				Severity:     "warning",
				Description:  fmt.Sprintf("New call path in %s: %s", rootSvc, summarizeEdges(edges)),
				DetectedAt:   ts,
			})
		}
	}

	if len(anomalies) > 0 {
		return s.backend.FlushAnomalies(ctx, anomalies)
	}
	return nil
}

// ── Error signature worker ────────────────────────────────────────────────────

func (s *Storage) errorSignatureWorker() {
	defer s.wg.Done()
	select {
	case <-time.After(2 * time.Minute):
	case <-s.ctx.Done():
		return
	}
	ticker := time.NewTicker(fpWindowDur)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := s.runErrorSignature(); err != nil {
				s.onError(fmt.Errorf("error signature: %w", err))
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Storage) runErrorSignature() error {
	ctx := s.ctx
	now := time.Now()
	toNs := now.UnixNano()
	fromNs := now.Add(-fpWindowDur).UnixNano()

	spans, err := s.backend.QuerySpans(ctx, SpanQuery{
		From: fromNs, To: toNs, StatusCode: 2, InternalLimit: 2000,
	})
	if err != nil {
		return err
	}

	existing, err := s.backend.QueryErrorSignatures(ctx, "")
	if err != nil {
		return err
	}
	baseline := make(map[string]ErrorSignatureRow)
	candidates := make(map[string]ErrorSignatureRow)
	for _, sig := range existing {
		if sig.IsBaseline {
			baseline[sig.Hash] = sig
		} else {
			candidates[sig.Hash] = sig
		}
	}

	// Bootstrap mode: silently promote all new error signatures on first pass.
	errBootstrapMode := len(baseline) == 0

	// Count occurrences per signature in this window
	type windowEntry struct {
		sig   ErrorSignatureRow
		count int64
	}
	window := make(map[string]*windowEntry)
	for _, sp := range spans {
		hash, sig := buildErrorSig(sp)
		if hash == "" {
			continue
		}
		if _, ok := window[hash]; !ok {
			window[hash] = &windowEntry{sig: sig}
		}
		window[hash].count++
	}

	var anomalies []AnomalyRow
	ts := now.Unix()

	for hash, we := range window {
		sig := we.sig
		sig.OccurrenceCount = we.count
		sig.LastSeenAt = ts

		if base, inBaseline := baseline[hash]; inBaseline {
			base.OccurrenceCount += we.count
			base.LastSeenAt = ts
			_ = s.backend.UpsertErrorSignature(ctx, base)
			// Spike: current count > N× baseline rate (baseline rate in occurrences/window)
			if base.BaselineRate > 0 && float64(we.count) > base.BaselineRate*errSpikeMultiple {
				anomalies = append(anomalies, AnomalyRow{
					EntityID:     sig.Service,
					SignalType:   "error_signature",
					DetectorName: "Error Signature Spike",
					MetricName:   "error.signature.rate",
					Value:        float64(we.count),
					Score:        float64(we.count) / base.BaselineRate,
					Mean:         base.BaselineRate,
					Severity:     "critical",
					Description:  fmt.Sprintf("Error spike on %s: %s (%dx above baseline)", sig.Service, sig.ErrorType, int(float64(we.count)/base.BaselineRate)),
					DetectedAt:   ts,
				})
			}
			continue
		}

		cand, isCandidate := candidates[hash]
		if !isCandidate {
			sig.FirstSeenAt = ts
			cand = sig
		} else {
			cand.OccurrenceCount += we.count
			cand.LastSeenAt = ts
		}

		// In bootstrap mode, silently promote without alerting.
		if errBootstrapMode {
			cand.IsBaseline = true
			cand.BaselineRate = float64(we.count)
			_ = s.backend.UpsertErrorSignature(ctx, cand)
			continue
		}

		age := time.Duration(ts-cand.FirstSeenAt) * time.Second
		if cand.OccurrenceCount >= errBaselineMinOcc && age >= errBaselineMinAge {
			cand.IsBaseline = true
			// rate = total occurrences / number of windows seen
			windows := float64(age) / float64(fpWindowDur)
			if windows < 1 {
				windows = 1
			}
			cand.BaselineRate = float64(cand.OccurrenceCount) / windows
			_ = s.backend.UpsertErrorSignature(ctx, cand)
			continue
		}
		_ = s.backend.UpsertErrorSignature(ctx, cand)

		// New signature: emit on first detection only
		if !isCandidate {
			sev := "warning"
			if we.count >= 5 {
				sev = "critical"
			}
			anomalies = append(anomalies, AnomalyRow{
				EntityID:     sig.Service,
				SignalType:   "error_signature",
				DetectorName: "New Error Signature",
				MetricName:   "error.signature.new",
				Value:        float64(we.count),
				Score:        float64(we.count),
				Severity:     sev,
				Description:  fmt.Sprintf("NEW error in %s: %s (http=%s, op=%s)", sig.Service, sig.ErrorType, sig.HTTPStatus, sig.Operation),
				DetectedAt:   ts,
			})
		}
	}

	if len(anomalies) > 0 {
		return s.backend.FlushAnomalies(ctx, anomalies)
	}
	return nil
}

// ── Span rate worker ──────────────────────────────────────────────────────────
// Computes per-service error rate and P95 latency every 5 min, runs through MAD detector.

func (s *Storage) spanRateWorker() {
	defer s.wg.Done()
	select {
	case <-time.After(3 * time.Minute):
	case <-s.ctx.Done():
		return
	}
	ticker := time.NewTicker(fpWindowDur)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := s.runSpanRateDetection(); err != nil {
				s.onError(fmt.Errorf("span rate detection: %w", err))
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Storage) runSpanRateDetection() error {
	ctx := s.ctx
	now := time.Now()
	toNs := now.UnixNano()
	fromNs := now.Add(-fpWindowDur).UnixNano()

	spans, err := s.backend.QuerySpans(ctx, SpanQuery{
		From: fromNs, To: toNs, InternalLimit: 5000,
	})
	if err != nil {
		return err
	}

	// Aggregate per service
	type svcStats struct {
		total    int
		errors   int
		durations []float64
	}
	byService := make(map[string]*svcStats)
	for _, sp := range spans {
		var res map[string]any
		json.Unmarshal([]byte(sp.ResourceAttrs), &res)
		svc, _ := res["service.name"].(string)
		if svc == "" {
			continue
		}
		st := byService[svc]
		if st == nil {
			st = &svcStats{}
			byService[svc] = st
		}
		st.total++
		if sp.StatusCode == 2 {
			st.errors++
		}
		st.durations = append(st.durations, sp.DurationMs)
	}

	var anomalies []AnomalyRow
	ts := now.Unix()

	for svc, st := range byService {
		if st.total < 5 {
			continue
		}
		// Error rate
		errRate := float64(st.errors) / float64(st.total)
		if a := s.detector.Check(svc, "span.error_rate", errRate); a != nil {
			a.SignalType = "span_error_rate"
			a.DetectorName = "Span Error Rate"
			a.Description = fmt.Sprintf("%s error rate %.1f%% deviates from baseline %.1f%% (score %.2f)",
				svc, errRate*100, a.Mean*100, a.Score)
			a.DetectedAt = ts
			anomalies = append(anomalies, *a)
		}
		// P95 latency
		if len(st.durations) >= 5 {
			p95 := calcP95(st.durations)
			if a := s.detector.Check(svc, "span.p95_latency_ms", p95); a != nil {
				a.SignalType = "span_latency"
				a.DetectorName = "P95 Latency Anomaly"
				a.Description = fmt.Sprintf("%s P95 latency %.0fms deviates from baseline %.0fms (score %.2f)",
					svc, p95, a.Mean, a.Score)
				a.DetectedAt = ts
				anomalies = append(anomalies, *a)
			}
		}
	}

	if len(anomalies) > 0 {
		return s.backend.FlushAnomalies(ctx, anomalies)
	}
	return nil
}

// ── Missing service worker ────────────────────────────────────────────────────

func (s *Storage) missingServiceWorker() {
	defer s.wg.Done()
	// Wait longer than the fingerprint worker so baseline is populated first.
	select {
	case <-time.After(4 * time.Minute):
	case <-s.ctx.Done():
		return
	}
	ticker := time.NewTicker(missingCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := s.checkMissingServices(); err != nil {
				s.onError(fmt.Errorf("missing service check: %w", err))
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Storage) checkMissingServices() error {
	ctx := s.ctx

	// Collect all root services that have been promoted to baseline.
	existing, err := s.backend.QueryTraceFingerprints(ctx, "")
	if err != nil {
		return err
	}
	baselineSvcs := make(map[string]bool)
	for _, fp := range existing {
		if fp.IsBaseline && fp.RootService != "" {
			baselineSvcs[fp.RootService] = true
		}
	}
	if len(baselineSvcs) == 0 {
		return nil
	}

	now := time.Now()
	var anomalies []AnomalyRow
	ts := now.Unix()

	s.missingMu.Lock()
	defer s.missingMu.Unlock()

	for svc := range baselineSvcs {
		lastSeen, ok := s.lastSeenRootSvc[svc]
		if !ok {
			// Never seen since this process started — skip to avoid false alerts on restart.
			continue
		}
		if now.Sub(lastSeen) < missingAlertThreshold {
			continue
		}
		if s.missingEmitted[svc] {
			continue // already alerted this absence period
		}
		anomalies = append(anomalies, AnomalyRow{
			EntityID:     svc,
			SignalType:   "missing_service",
			DetectorName: "Missing Service",
			MetricName:   "trace.missing_service",
			Value:        1,
			Score:        1,
			Severity:     "critical",
			Description:  fmt.Sprintf("%s has gone silent — no traces seen for %.0f minutes", svc, now.Sub(lastSeen).Minutes()),
			DetectedAt:   ts,
		})
		s.missingEmitted[svc] = true
	}

	if len(anomalies) > 0 {
		return s.backend.FlushAnomalies(ctx, anomalies)
	}
	return nil
}

// ── Fingerprint builders ──────────────────────────────────────────────────────

type fpSpan struct {
	SpanID       string
	ParentSpanID string
	ServiceName  string
	OpName       string
}

// buildTraceFP computes a structural fingerprint for a set of spans belonging
// to one trace. Returns the hash, cross-service edges, root service name, and
// root operation name. Returns empty hash if no cross-service edges are found.
func buildTraceFP(spans []SpanRow) (hash string, edges []string, rootSvc, rootOp string) {
	byID := make(map[string]fpSpan, len(spans))
	for _, s := range spans {
		var res map[string]any
		json.Unmarshal([]byte(s.ResourceAttrs), &res)
		svc, _ := res["service.name"].(string)
		byID[s.SpanID] = fpSpan{
			SpanID: s.SpanID, ParentSpanID: s.ParentSpanID,
			ServiceName: svc, OpName: s.Name,
		}
	}
	for _, s := range byID {
		isRoot := s.ParentSpanID == "" || s.ParentSpanID == "0000000000000000"
		if isRoot && rootSvc == "" {
			rootSvc = s.ServiceName
			rootOp = s.OpName
		}
		parent, ok := byID[s.ParentSpanID]
		if !ok || parent.ServiceName == "" || parent.ServiceName == s.ServiceName {
			continue
		}
		edges = append(edges, parent.ServiceName+":"+parent.OpName+"→"+s.ServiceName+":"+s.OpName)
	}
	if len(edges) == 0 {
		return "", nil, rootSvc, rootOp
	}
	sort.Strings(edges)
	h := md5.Sum([]byte(strings.Join(edges, "|")))
	hash = hex.EncodeToString(h[:])[:16]
	return
}

func buildErrorSig(s SpanRow) (hash string, sig ErrorSignatureRow) {
	var spanAttrs map[string]any
	var resAttrs map[string]any
	json.Unmarshal([]byte(s.SpanAttrs), &spanAttrs)
	json.Unmarshal([]byte(s.ResourceAttrs), &resAttrs)

	svc, _ := resAttrs["service.name"].(string)
	errType := ""
	for _, k := range []string{"exception.type", "error.kind", "error.type"} {
		if v, ok := spanAttrs[k].(string); ok && v != "" {
			errType = v
			break
		}
	}
	if errType == "" {
		errType = "error"
	}
	httpStatus := ""
	for _, k := range []string{"http.response.status_code", "http.status_code"} {
		if v, ok := spanAttrs[k]; ok {
			httpStatus = fmt.Sprint(v)
			break
		}
	}
	h := md5.Sum([]byte(svc + "|" + errType + "|" + httpStatus + "|" + s.Name))
	hash = hex.EncodeToString(h[:])[:16]
	sig = ErrorSignatureRow{
		Hash: hash, Service: svc, ErrorType: errType,
		HTTPStatus: httpStatus, Operation: s.Name,
	}
	return
}

func summarizeEdges(edges []string) string {
	if len(edges) <= 2 {
		return strings.Join(edges, ", ")
	}
	return strings.Join(edges[:2], ", ") + fmt.Sprintf(" +%d more", len(edges)-2)
}

func calcP95(durations []float64) float64 {
	if len(durations) == 0 {
		return 0
	}
	sorted := make([]float64, len(durations))
	copy(sorted, durations)
	sort.Float64s(sorted)
	idx := int(float64(len(sorted)) * 0.95)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
