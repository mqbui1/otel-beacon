package storage

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

// GenAISpanRow represents a processed gen_ai.* span with extracted semantic fields.
type GenAISpanRow struct {
	TraceID       string  `json:"trace_id"`
	SpanID        string  `json:"span_id"`
	ParentSpanID  string  `json:"parent_span_id,omitempty"`
	EntityID      string  `json:"entity_id"`
	System        string  `json:"system"`              // gen_ai.system (openai, anthropic, …)
	Operation     string  `json:"operation"`           // gen_ai.operation.name
	Model         string  `json:"model"`               // gen_ai.request.model
	AgentName     string  `json:"agent_name,omitempty"` // gen_ai.agent.name
	ToolName      string  `json:"tool_name,omitempty"`  // gen_ai.tool.name
	InputTokens   int64   `json:"input_tokens"`
	OutputTokens  int64   `json:"output_tokens"`
	TotalCostUSD  float64 `json:"total_cost_usd"`
	StartNs       int64   `json:"start_ns"`
	DurationMs    float64 `json:"duration_ms"`
	StatusCode    int     `json:"status_code"`
	Prompt        string  `json:"prompt,omitempty"`     // from gen_ai.user.message event
	Completion    string  `json:"completion,omitempty"` // from gen_ai.assistant.message event
	SpanAttrs     string  `json:"span_attrs"`
	ResourceAttrs string  `json:"resource_attrs"`
}

// EvalResultRow holds LLM-as-judge quality scores for a single GenAI span.
type EvalResultRow struct {
	SpanID        string  `json:"span_id"`
	TraceID       string  `json:"trace_id"`
	Hallucination float64 `json:"hallucination"`  // 0–1 (higher = more likely hallucinated)
	Coherence     float64 `json:"coherence"`      // 0–1 (higher = more coherent)
	Relevance     float64 `json:"relevance"`      // 0–1 (higher = more relevant)
	Toxicity      float64 `json:"toxicity"`       // 0–1 (higher = more toxic)
	OverallScore  float64 `json:"overall_score"`  // 0–1 composite
	Reasoning     string  `json:"reasoning"`
	EvaluatedAt   int64   `json:"evaluated_at"`
}

// GuardrailEventRow records a triggered guardrail check.
type GuardrailEventRow struct {
	SpanID    string `json:"span_id"`
	TraceID   string `json:"trace_id"`
	CheckType string `json:"check_type"` // "prompt_injection","pii","toxicity"
	Triggered bool   `json:"triggered"`
	Severity  string `json:"severity"` // "low","medium","high"
	Detail    string `json:"detail"`
	CheckedAt int64  `json:"checked_at"`
}

// GenAIAgentRow holds aggregated stats for a single agent.
type GenAIAgentRow struct {
	AgentName     string  `json:"agent_name"`
	EntityID      string  `json:"entity_id"`
	SpanCount     int64   `json:"span_count"`
	TotalCostUSD  float64 `json:"total_cost_usd"`
	TotalTokens   int64   `json:"total_tokens"`
	AvgDurationMs float64 `json:"avg_duration_ms"`
	ErrorRate     float64 `json:"error_rate"`
	LastSeenAt    int64   `json:"last_seen_at"`
}

// GenAICostSummary holds cost rolled up by a single dimension.
type GenAICostSummary struct {
	Dimension    string  `json:"dimension"`     // model name / agent name / service name
	GroupBy      string  `json:"group_by"`      // "model" | "agent" | "service"
	TotalCostUSD float64 `json:"total_cost_usd"`
	TotalTokens  int64   `json:"total_tokens"`
	SpanCount    int64   `json:"span_count"`
}

// GenAIQuery is the filter passed to QueryGenAISpans.
type GenAIQuery struct {
	TraceID   string
	AgentName string
	Model     string
	System    string
	From      int64
	To        int64
	Limit     int
}

// ---------------------------------------------------------------------------
// Model pricing table (USD per 1 M tokens, mid-2025 list prices)
// ---------------------------------------------------------------------------

type modelPrice struct{ inputPer1M, outputPer1M float64 }

var modelPricing = map[string]modelPrice{
	// OpenAI
	"gpt-4o":             {2.50, 10.00},
	"gpt-4o-mini":        {0.15, 0.60},
	"gpt-4-turbo":        {10.00, 30.00},
	"gpt-4":              {30.00, 60.00},
	"gpt-3.5-turbo":      {0.50, 1.50},
	"o1":                 {15.00, 60.00},
	"o1-mini":            {3.00, 12.00},
	// Anthropic
	"claude-3-5-sonnet-20241022": {3.00, 15.00},
	"claude-3-5-sonnet":          {3.00, 15.00},
	"claude-3-5-haiku":           {0.80, 4.00},
	"claude-3-opus":              {15.00, 75.00},
	"claude-3-sonnet":            {3.00, 15.00},
	"claude-3-haiku":             {0.25, 1.25},
	"claude-2":                   {8.00, 24.00},
	// Google
	"gemini-1.5-pro":   {1.25, 5.00},
	"gemini-1.5-flash": {0.075, 0.30},
	"gemini-2.0-flash": {0.10, 0.40},
	"gemini-pro":       {0.50, 1.50},
	// Meta Llama
	"llama-3.1-70b-instruct": {0.59, 0.79},
	"llama-3.1-8b-instruct":  {0.20, 0.20},
	"llama-3.2-90b":          {0.72, 0.72},
	// Mistral
	"mistral-large": {2.00, 6.00},
	"mistral-small": {0.20, 0.60},
	"mistral-7b":    {0.25, 0.25},
	// Cohere
	"command-r-plus": {3.00, 15.00},
	"command-r":      {0.50, 1.50},
}

func calcCost(model string, inputTokens, outputTokens int64) float64 {
	p, ok := modelPricing[strings.ToLower(model)]
	if !ok {
		p = modelPrice{1.00, 1.00} // unknown model: $1/1M fallback
	}
	return (float64(inputTokens)*p.inputPer1M + float64(outputTokens)*p.outputPer1M) / 1_000_000
}

// ---------------------------------------------------------------------------
// GenAI span extraction from ptrace
// ---------------------------------------------------------------------------

// isGenAISpan returns true when the span carries gen_ai.* semantic convention attributes.
func isGenAISpan(sp ptrace.Span) bool {
	found := false
	sp.Attributes().Range(func(k string, _ pcommon.Value) bool {
		if strings.HasPrefix(k, "gen_ai.") {
			found = true
			return false
		}
		return true
	})
	return found
}

// extractGenAISpan builds a GenAISpanRow from a ptrace.Span and its resource JSON.
func extractGenAISpan(sp ptrace.Span, entityID, resJSON string) GenAISpanRow {
	startNs := int64(sp.StartTimestamp())
	endNs := int64(sp.EndTimestamp())

	attrs := make(map[string]any, sp.Attributes().Len())
	sp.Attributes().Range(func(k string, v pcommon.Value) bool {
		attrs[k] = v.AsRaw()
		return true
	})

	getStr := func(k string) string {
		if v, ok := attrs[k]; ok {
			if s, ok := v.(string); ok {
				return s
			}
		}
		return ""
	}
	getInt := func(k string) int64 {
		if v, ok := attrs[k]; ok {
			switch t := v.(type) {
			case int64:
				return t
			case float64:
				return int64(t)
			}
		}
		return 0
	}

	inputTok := getInt("gen_ai.usage.input_tokens")
	outputTok := getInt("gen_ai.usage.output_tokens")
	model := getStr("gen_ai.request.model")
	if model == "" {
		model = getStr("gen_ai.response.model")
	}

	// Extract prompt / completion from span events.
	var prompt, completion string
	for i := 0; i < sp.Events().Len(); i++ {
		ev := sp.Events().At(i)
		switch ev.Name() {
		case "gen_ai.user.message":
			if v, ok := ev.Attributes().Get("content"); ok {
				prompt = v.AsString()
			}
		case "gen_ai.assistant.message":
			if v, ok := ev.Attributes().Get("content"); ok {
				completion = v.AsString()
			}
		}
	}
	// Prevent runaway DB growth — truncate at 4 KB.
	if len(prompt) > 4096 {
		prompt = prompt[:4096] + "…"
	}
	if len(completion) > 4096 {
		completion = completion[:4096] + "…"
	}

	spanAttrsJSON, _ := json.Marshal(attrs)

	return GenAISpanRow{
		TraceID:       sp.TraceID().String(),
		SpanID:        sp.SpanID().String(),
		ParentSpanID:  sp.ParentSpanID().String(),
		EntityID:      entityID,
		System:        getStr("gen_ai.system"),
		Operation:     getStr("gen_ai.operation.name"),
		Model:         model,
		AgentName:     getStr("gen_ai.agent.name"),
		ToolName:      getStr("gen_ai.tool.name"),
		InputTokens:   inputTok,
		OutputTokens:  outputTok,
		TotalCostUSD:  calcCost(model, inputTok, outputTok),
		StartNs:       startNs,
		DurationMs:    float64(endNs-startNs) / 1e6,
		StatusCode:    int(sp.Status().Code()),
		Prompt:        prompt,
		Completion:    completion,
		SpanAttrs:     string(spanAttrsJSON),
		ResourceAttrs: resJSON,
	}
}

// ---------------------------------------------------------------------------
// GenAI worker — drains genaiCh, flushes to DB, runs guardrails, enqueues eval
// ---------------------------------------------------------------------------

func (s *Storage) genaiWorker() {
	defer s.wg.Done()
	batch := make([]GenAISpanRow, 0, s.batchSize)
	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		s.withRetry("genai", func() error {
			return s.backend.FlushGenAISpans(s.ctx, batch)
		})

		// Run drift detection on cost and latency using the shared detector.
		for _, gs := range batch {
			if gs.Model != "" {
				if a := s.detector.Check(gs.EntityID, "gen_ai.cost:"+gs.Model, gs.TotalCostUSD); a != nil {
					a.SignalType = "genai_cost_spike"
					a.DetectorName = "genai_cost"
					a.Description = "GenAI cost spike for model " + gs.Model
					s.withRetry("anomalies", func() error {
						return s.backend.FlushAnomalies(s.ctx, []AnomalyRow{*a})
					})
					s.onAnomaly(*a)
				}
				if a := s.detector.Check(gs.EntityID, "gen_ai.latency:"+gs.Model, gs.DurationMs); a != nil {
					a.SignalType = "genai_latency_drift"
					a.DetectorName = "genai_latency"
					a.Description = "GenAI latency drift for model " + gs.Model
					s.withRetry("anomalies", func() error {
						return s.backend.FlushAnomalies(s.ctx, []AnomalyRow{*a})
					})
					s.onAnomaly(*a)
				}
				if gs.InputTokens > 0 {
					if a := s.detector.Check(gs.EntityID, "gen_ai.input_tokens:"+gs.Model, float64(gs.InputTokens)); a != nil {
						a.SignalType = "genai_context_bloat"
						a.DetectorName = "genai_context"
						a.Description = "GenAI context bloat for model " + gs.Model
						s.withRetry("anomalies", func() error {
							return s.backend.FlushAnomalies(s.ctx, []AnomalyRow{*a})
						})
						s.onAnomaly(*a)
					}
				}
			}
		}

		// Guardrail checks (fast, regex-based — < 1 ms per span).
		var guardrailEvents []GuardrailEventRow
		for _, gs := range batch {
			guardrailEvents = append(guardrailEvents, checkGuardrails(gs)...)
		}
		if len(guardrailEvents) > 0 {
			s.backend.FlushGuardrailEvents(s.ctx, guardrailEvents) //nolint:errcheck
		}

		// Enqueue to async LLM evaluator (non-blocking — drop under backpressure).
		for _, gs := range batch {
			if gs.Prompt != "" || gs.Completion != "" {
				select {
				case s.genaiEvalCh <- gs:
				default:
				}
			}
		}
		batch = batch[:0]
	}

	for {
		select {
		case row, ok := <-s.genaiCh:
			if !ok {
				flush()
				return
			}
			batch = append(batch, row)
			if len(batch) >= s.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// ---------------------------------------------------------------------------
// Storage passthrough — GenAI query/flush methods
// ---------------------------------------------------------------------------

func (s *Storage) QueryGenAISpans(ctx context.Context, q GenAIQuery) ([]GenAISpanRow, error) {
	return s.backend.QueryGenAISpans(ctx, q)
}

func (s *Storage) QueryGenAIAgents(ctx context.Context, from, to int64) ([]GenAIAgentRow, error) {
	return s.backend.QueryGenAIAgents(ctx, from, to)
}

func (s *Storage) QueryGenAICosts(ctx context.Context, from, to int64, groupBy string) ([]GenAICostSummary, error) {
	return s.backend.QueryGenAICosts(ctx, from, to, groupBy)
}

func (s *Storage) QueryEvalResults(ctx context.Context, traceID string, lim int) ([]EvalResultRow, error) {
	return s.backend.QueryEvalResults(ctx, traceID, lim)
}

func (s *Storage) QueryGuardrailEvents(ctx context.Context, traceID string, lim int) ([]GuardrailEventRow, error) {
	return s.backend.QueryGuardrailEvents(ctx, traceID, lim)
}

func (s *Storage) FlushEvalResults(ctx context.Context, batch []EvalResultRow) error {
	return s.backend.FlushEvalResults(ctx, batch)
}

// GenAIEvalCh returns the channel that the server-side eval worker drains.
func (s *Storage) GenAIEvalCh() <-chan GenAISpanRow {
	return s.genaiEvalCh
}
