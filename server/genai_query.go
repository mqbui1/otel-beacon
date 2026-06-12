package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/yourorg/otel-backend/storage"
)

// registerGenAIRoutes wires up all /v1/genai/* handlers onto the provided mux.
func registerGenAIRoutes(mux *http.ServeMux, store *storage.Storage, ew *ExperimentWorker, logger *zap.Logger) {
	g := &genaiServer{store: store, ew: ew, logger: logger}
	mux.HandleFunc("/v1/genai/spans", g.spans)
	mux.HandleFunc("/v1/genai/agents", g.agents)
	mux.HandleFunc("/v1/genai/costs", g.costs)
	mux.HandleFunc("/v1/genai/eval", g.eval)
	mux.HandleFunc("/v1/genai/guardrails", g.guardrailEvents)
	mux.HandleFunc("/v1/genai/guardrails/check", g.guardrailCheck)
	mux.HandleFunc("/v1/genai/trace/", g.traceWaterfall)   // /v1/genai/trace/{trace_id}
	mux.HandleFunc("/v1/genai/sessions", g.sessions)        // GET /v1/genai/sessions?entity=&limit=
	mux.HandleFunc("/v1/genai/sessions/", g.sessionDetail)  // GET /v1/genai/sessions/{session_id}
	mux.HandleFunc("/v1/datasets", g.datasets)                          // GET list / POST create
	mux.HandleFunc("/v1/datasets/", g.datasetDetail)                    // GET /v1/datasets/{id}
	mux.HandleFunc("/v1/experiments", g.experiments)                    // GET list / POST create
	mux.HandleFunc("/v1/experiments/", g.experimentDetail)              // GET /v1/experiments/{id}
	// Custom metrics + autotune
	mux.HandleFunc("/v1/genai/custom-metrics", g.customMetrics)         // GET list / POST create
	mux.HandleFunc("/v1/genai/custom-metrics/run/", g.runCustomMetric)  // POST /v1/genai/custom-metrics/run/{id}
	mux.HandleFunc("/v1/genai/custom-metrics/results", g.customMetricResults) // GET results
	mux.HandleFunc("/v1/genai/eval/feedback", g.evalFeedback)           // GET/POST feedback
}

type genaiServer struct {
	store  *storage.Storage
	ew     *ExperimentWorker
	logger *zap.Logger
}

// GET /v1/genai/spans?trace_id=&agent=&model=&system=&from=&to=&limit=
func (g *genaiServer) spans(w http.ResponseWriter, r *http.Request) {
	q := storage.GenAIQuery{
		TraceID:   r.URL.Query().Get("trace_id"),
		AgentName: r.URL.Query().Get("agent"),
		Model:     r.URL.Query().Get("model"),
		System:    r.URL.Query().Get("system"),
		From:      parseInt64(r.URL.Query().Get("from")),
		To:        parseInt64(r.URL.Query().Get("to")),
		Limit:     parseInt(r.URL.Query().Get("limit")),
	}
	rows, err := g.store.QueryGenAISpans(r.Context(), q)
	writeJSON(w, rows, err, g.logger)
}

// GET /v1/genai/trace/{trace_id} — all gen_ai spans for one trace as a waterfall.
func (g *genaiServer) traceWaterfall(w http.ResponseWriter, r *http.Request) {
	traceID := strings.TrimPrefix(r.URL.Path, "/v1/genai/trace/")
	if traceID == "" {
		http.Error(w, "missing trace_id", http.StatusBadRequest)
		return
	}
	rows, err := g.store.QueryGenAISpans(r.Context(), storage.GenAIQuery{
		TraceID: traceID,
		Limit:   500,
	})
	if err != nil {
		writeJSON(w, nil, err, g.logger)
		return
	}
	// Also fetch eval results and guardrail events for this trace.
	evals, _ := g.store.QueryEvalResults(r.Context(), traceID, 500)
	guardrails, _ := g.store.QueryGuardrailEvents(r.Context(), traceID, 500)

	type waterfall struct {
		Spans      []storage.GenAISpanRow       `json:"spans"`
		Evals      []storage.EvalResultRow      `json:"evals"`
		Guardrails []storage.GuardrailEventRow  `json:"guardrails"`
	}
	writeJSON(w, waterfall{
		Spans:      rows,
		Evals:      evals,
		Guardrails: guardrails,
	}, nil, g.logger)
}

// GET /v1/genai/agents?from=&to=
func (g *genaiServer) agents(w http.ResponseWriter, r *http.Request) {
	from := parseInt64(r.URL.Query().Get("from"))
	to := parseInt64(r.URL.Query().Get("to"))
	rows, err := g.store.QueryGenAIAgents(r.Context(), from, to)
	writeJSON(w, rows, err, g.logger)
}

// GET /v1/genai/costs?from=&to=&group_by=model|agent|service
func (g *genaiServer) costs(w http.ResponseWriter, r *http.Request) {
	from := parseInt64(r.URL.Query().Get("from"))
	to := parseInt64(r.URL.Query().Get("to"))
	groupBy := r.URL.Query().Get("group_by")
	rows, err := g.store.QueryGenAICosts(r.Context(), from, to, groupBy)
	writeJSON(w, rows, err, g.logger)
}

// GET /v1/genai/eval?trace_id=&limit=
func (g *genaiServer) eval(w http.ResponseWriter, r *http.Request) {
	traceID := r.URL.Query().Get("trace_id")
	lim := parseInt(r.URL.Query().Get("limit"))
	rows, err := g.store.QueryEvalResults(r.Context(), traceID, lim)
	writeJSON(w, rows, err, g.logger)
}

// GET /v1/genai/guardrails?trace_id=&limit=
func (g *genaiServer) guardrailEvents(w http.ResponseWriter, r *http.Request) {
	traceID := r.URL.Query().Get("trace_id")
	lim := parseInt(r.URL.Query().Get("limit"))
	rows, err := g.store.QueryGuardrailEvents(r.Context(), traceID, lim)
	writeJSON(w, rows, err, g.logger)
}

// GET /v1/genai/sessions?entity=&limit=
func (g *genaiServer) sessions(w http.ResponseWriter, r *http.Request) {
	entity := r.URL.Query().Get("entity")
	lim := parseInt(r.URL.Query().Get("limit"))
	rows, err := g.store.QuerySessions(r.Context(), entity, lim)
	writeJSON(w, rows, err, g.logger)
}

// GET /v1/genai/sessions/{session_id}
func (g *genaiServer) sessionDetail(w http.ResponseWriter, r *http.Request) {
	sessionID := strings.TrimPrefix(r.URL.Path, "/v1/genai/sessions/")
	if sessionID == "" {
		http.Error(w, "missing session_id", http.StatusBadRequest)
		return
	}
	sess, err := g.store.QuerySession(r.Context(), sessionID)
	if err != nil {
		writeJSON(w, nil, err, g.logger)
		return
	}
	spans, _ := g.store.QueryGenAISpans(r.Context(), storage.GenAIQuery{SessionID: sessionID, Limit: 500})
	evals, _ := g.store.QueryEvalResults(r.Context(), "", 500)

	// Filter evals to only those belonging to spans in this session.
	spanSet := make(map[string]bool, len(spans))
	for _, sp := range spans {
		spanSet[sp.SpanID] = true
	}
	var sessionEvals []storage.EvalResultRow
	for _, ev := range evals {
		if spanSet[ev.SpanID] {
			sessionEvals = append(sessionEvals, ev)
		}
	}

	type sessionDetailResponse struct {
		Session *storage.SessionRow      `json:"session"`
		Spans   []storage.GenAISpanRow   `json:"spans"`
		Evals   []storage.EvalResultRow  `json:"evals"`
	}
	writeJSON(w, sessionDetailResponse{Session: sess, Spans: spans, Evals: sessionEvals}, nil, g.logger)
}

// POST /v1/genai/guardrails/check
// Body: {"prompt":"…","completion":"…","trace_id":"…","span_id":"…"}
//
// Two-phase check:
//  1. Hardcoded regex checks (PII, prompt injection, toxicity) — always fast, no LLM.
//  2. Custom metrics with action=block — run in parallel via LLM with a 5 s budget.
//     Fail-open: if LLM is unavailable or times out the request is NOT blocked.
func (g *genaiServer) guardrailCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var req storage.CheckGuardrailsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Phase 1: hardcoded regex checks — synchronous, no LLM.
	resp := storage.RunGuardrailCheck(req)

	// Phase 2: custom block metrics — parallel LLM, 5 s budget.
	if customEvents := runCustomBlockChecks(r.Context(), req, g.store, g.logger); len(customEvents) > 0 {
		resp.Events = append(resp.Events, customEvents...)
		resp.Triggered = true
	}

	writeJSON(w, resp, nil, g.logger)
}

// ---------------------------------------------------------------------------
// Datasets
// ---------------------------------------------------------------------------

// GET /v1/datasets?entity=&limit=   POST /v1/datasets
func (g *genaiServer) datasets(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		g.createDataset(w, r)
		return
	}
	entity := r.URL.Query().Get("entity")
	lim := parseInt(r.URL.Query().Get("limit"))
	rows, err := g.store.ListDatasets(r.Context(), entity, lim)
	writeJSON(w, rows, err, g.logger)
}

// POST /v1/datasets body: {"name":"…","entity_id":"…","rows":[{"prompt":"…","completion":"…"},...]}
func (g *genaiServer) createDataset(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name     string `json:"name"`
		EntityID string `json:"entity_id"`
		Rows     []struct {
			Prompt     string `json:"prompt"`
			Completion string `json:"completion"`
			Context    string `json:"context"`
			Expected   string `json:"expected"`
		} `json:"rows"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Name == "" || len(req.Rows) == 0 {
		http.Error(w, "name and rows are required", http.StatusBadRequest)
		return
	}
	datasetID := fmt.Sprintf("ds-%d", time.Now().UnixNano())
	meta := storage.DatasetMeta{
		DatasetID: datasetID,
		Name:      req.Name,
		EntityID:  req.EntityID,
		RowCount:  len(req.Rows),
	}
	rows := make([]storage.DatasetRow, len(req.Rows))
	for i, rr := range req.Rows {
		rows[i] = storage.DatasetRow{
			RowID:      fmt.Sprintf("%s-r%d", datasetID, i),
			DatasetID:  datasetID,
			Prompt:     rr.Prompt,
			Completion: rr.Completion,
			Context:    rr.Context,
			Expected:   rr.Expected,
		}
	}
	if err := g.store.CreateDataset(r.Context(), meta, rows); err != nil {
		writeJSON(w, nil, err, g.logger)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(meta)
}

// GET /v1/datasets/{id}
func (g *genaiServer) datasetDetail(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/datasets/")
	if id == "" {
		http.Error(w, "missing dataset_id", http.StatusBadRequest)
		return
	}
	meta, rows, err := g.store.GetDataset(r.Context(), id)
	if err != nil {
		writeJSON(w, nil, err, g.logger)
		return
	}
	if meta == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"dataset": meta, "rows": rows})
}

// ---------------------------------------------------------------------------
// Experiments
// ---------------------------------------------------------------------------

// GET /v1/experiments?entity=&limit=   POST /v1/experiments
func (g *genaiServer) experiments(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		g.createExperiment(w, r)
		return
	}
	entity := r.URL.Query().Get("entity")
	lim := parseInt(r.URL.Query().Get("limit"))
	rows, err := g.store.ListExperiments(r.Context(), entity, lim)
	writeJSON(w, rows, err, g.logger)
}

// POST /v1/experiments body: {"name":"…","dataset_id":"…","entity_id":"…"}
func (g *genaiServer) createExperiment(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name      string `json:"name"`
		DatasetID string `json:"dataset_id"`
		EntityID  string `json:"entity_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.DatasetID == "" {
		http.Error(w, "dataset_id is required", http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		req.Name = "Experiment " + req.DatasetID
	}

	// Look up dataset to get row count.
	meta, _, err := g.store.GetDataset(r.Context(), req.DatasetID)
	if err != nil || meta == nil {
		http.Error(w, "dataset not found", http.StatusBadRequest)
		return
	}

	exp := storage.ExperimentRow{
		ExperimentID: fmt.Sprintf("exp-%d", time.Now().UnixNano()),
		Name:         req.Name,
		DatasetID:    req.DatasetID,
		EntityID:     req.EntityID,
		Status:       "pending",
		RowCount:     meta.RowCount,
	}
	if err := g.store.CreateExperiment(r.Context(), exp); err != nil {
		writeJSON(w, nil, err, g.logger)
		return
	}
	if g.ew != nil {
		g.ew.Enqueue(exp)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(exp)
}

// GET /v1/experiments/{id}
func (g *genaiServer) experimentDetail(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/experiments/")
	if id == "" {
		http.Error(w, "missing experiment_id", http.StatusBadRequest)
		return
	}
	exp, err := g.store.GetExperiment(r.Context(), id)
	if err != nil {
		writeJSON(w, nil, err, g.logger)
		return
	}
	if exp == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	results, err := g.store.GetExperimentResults(r.Context(), id)
	if err != nil {
		writeJSON(w, nil, err, g.logger)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"experiment": exp, "results": results})
}

// ---------------------------------------------------------------------------
// Custom Metrics
// ---------------------------------------------------------------------------

// GET /v1/genai/custom-metrics   POST /v1/genai/custom-metrics
func (g *genaiServer) customMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var req storage.CustomMetricDef
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		if req.Name == "" || req.Prompt == "" {
			http.Error(w, "name and prompt are required", http.StatusBadRequest)
			return
		}
		if req.OutputType == "" {
			req.OutputType = "boolean"
		}
		if req.ApplyTo == "" {
			req.ApplyTo = "span"
		}
		if req.Action == "" {
			req.Action = "alert"
		}
		req.MetricID = fmt.Sprintf("cm-%d", time.Now().UnixNano())
		if err := g.store.CreateCustomMetric(r.Context(), req); err != nil {
			writeJSON(w, nil, err, g.logger)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(req)
		return
	}
	metrics, err := g.store.ListCustomMetrics(r.Context())
	writeJSON(w, metrics, err, g.logger)
}

// POST /v1/genai/custom-metrics/run/{metric_id}?limit=50
func (g *genaiServer) runCustomMetric(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	metricID := strings.TrimPrefix(r.URL.Path, "/v1/genai/custom-metrics/run/")
	if metricID == "" {
		http.Error(w, "missing metric_id", http.StatusBadRequest)
		return
	}
	metrics, err := g.store.ListCustomMetrics(r.Context())
	if err != nil {
		writeJSON(w, nil, err, g.logger)
		return
	}
	var target *storage.CustomMetricDef
	for i := range metrics {
		if metrics[i].MetricID == metricID {
			target = &metrics[i]
			break
		}
	}
	if target == nil {
		http.Error(w, "metric not found", http.StatusNotFound)
		return
	}
	limit := parseInt(r.URL.Query().Get("limit"))
	results, err := RunCustomMetricOnRecentSpans(r.Context(), *target, g.store, g.logger, limit)
	writeJSON(w, results, err, g.logger)
}

// GET /v1/genai/custom-metrics/results?span_id=&metric_id=&limit=
func (g *genaiServer) customMetricResults(w http.ResponseWriter, r *http.Request) {
	spanID := r.URL.Query().Get("span_id")
	metricID := r.URL.Query().Get("metric_id")
	limit := parseInt(r.URL.Query().Get("limit"))
	rows, err := g.store.QueryCustomMetricResults(r.Context(), spanID, metricID, limit)
	writeJSON(w, rows, err, g.logger)
}

// ---------------------------------------------------------------------------
// Autotune / Eval Feedback
// ---------------------------------------------------------------------------

// GET  /v1/genai/eval/feedback?span_id=
// POST /v1/genai/eval/feedback  body: EvalFeedback JSON
func (g *genaiServer) evalFeedback(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var req storage.EvalFeedback
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		if req.SpanID == "" || req.MetricName == "" {
			http.Error(w, "span_id and metric_name are required", http.StatusBadRequest)
			return
		}
		req.FeedbackID = fmt.Sprintf("fb-%d", time.Now().UnixNano())
		if err := g.store.SaveEvalFeedback(r.Context(), req); err != nil {
			writeJSON(w, nil, err, g.logger)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(req)
		return
	}
	spanID := r.URL.Query().Get("span_id")
	rows, err := g.store.QueryEvalFeedback(r.Context(), spanID)
	writeJSON(w, rows, err, g.logger)
}

// ---------------------------------------------------------------------------
// runCustomBlockChecks — Phase 2 of guardrail check.
//
// Loads all custom metrics with action=block, runs them concurrently against
// the request prompt/completion with a 5-second shared timeout, and returns
// any triggered GuardrailEventRows. Fails open: LLM errors are silently
// ignored so a slow or unavailable model never blocks the request.
// ---------------------------------------------------------------------------

func runCustomBlockChecks(ctx context.Context, req storage.CheckGuardrailsRequest,
	store *storage.Storage, logger *zap.Logger) []storage.GuardrailEventRow {

	metrics, err := store.ListCustomMetrics(ctx)
	if err != nil {
		return nil
	}

	var blockMetrics []storage.CustomMetricDef
	for _, m := range metrics {
		if m.Action == "block" && m.ApplyTo == "span" {
			blockMetrics = append(blockMetrics, m)
		}
	}
	if len(blockMetrics) == 0 {
		return nil
	}

	gs := storage.GenAISpanRow{
		SpanID:     req.SpanID,
		TraceID:    req.TraceID,
		Prompt:     req.Prompt,
		Completion: req.Completion,
	}

	// Shared 5-second budget across all custom metrics.
	evalCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	type item struct{ event *storage.GuardrailEventRow }
	ch := make(chan item, len(blockMetrics))

	for _, m := range blockMetrics {
		m := m
		go func() {
			result := runCustomMetricEval(evalCtx, m, gs, nil, "")
			if result.ValueBool != nil && *result.ValueBool {
				ch <- item{event: &storage.GuardrailEventRow{
					TraceID:   gs.TraceID,
					SpanID:    gs.SpanID,
					CheckType: "custom_metric:" + m.Name,
					Triggered: true,
					Severity:  "high",
					Detail:    "[block] " + result.Reasoning,
					CheckedAt: time.Now().UnixNano(),
				}}
			} else {
				ch <- item{event: nil}
			}
		}()
	}

	var events []storage.GuardrailEventRow
	for range blockMetrics {
		if it := <-ch; it.event != nil {
			events = append(events, *it.event)
		}
	}

	if len(events) > 0 {
		logger.Info("custom block metrics triggered",
			zap.Int("count", len(events)),
			zap.String("span_id", req.SpanID))
		// Persist the violations for observability.
		_ = store.FlushGuardrailEvents(ctx, events)
	}
	return events
}

