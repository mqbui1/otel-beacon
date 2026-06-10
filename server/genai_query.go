package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"go.uber.org/zap"

	"github.com/yourorg/otel-backend/storage"
)

// registerGenAIRoutes wires up all /v1/genai/* handlers onto the provided mux.
func registerGenAIRoutes(mux *http.ServeMux, store *storage.Storage, logger *zap.Logger) {
	g := &genaiServer{store: store, logger: logger}
	mux.HandleFunc("/v1/genai/spans", g.spans)
	mux.HandleFunc("/v1/genai/agents", g.agents)
	mux.HandleFunc("/v1/genai/costs", g.costs)
	mux.HandleFunc("/v1/genai/eval", g.eval)
	mux.HandleFunc("/v1/genai/guardrails", g.guardrailEvents)
	mux.HandleFunc("/v1/genai/guardrails/check", g.guardrailCheck)
	mux.HandleFunc("/v1/genai/trace/", g.traceWaterfall)   // /v1/genai/trace/{trace_id}
	mux.HandleFunc("/v1/genai/sessions", g.sessions)       // GET /v1/genai/sessions?entity=&limit=
	mux.HandleFunc("/v1/genai/sessions/", g.sessionDetail) // GET /v1/genai/sessions/{session_id}
}

type genaiServer struct {
	store  *storage.Storage
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
	resp := storage.RunGuardrailCheck(req)
	writeJSON(w, resp, nil, g.logger)
}
