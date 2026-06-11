package storage

import "context"

// CustomMetricDef is a user-defined LLM-judge metric stored in the database.
type CustomMetricDef struct {
	MetricID    string `json:"metric_id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Prompt      string `json:"prompt"`
	OutputType  string `json:"output_type"` // "boolean" | "score"
	ApplyTo     string `json:"apply_to"`    // "span" | "session"
	Action      string `json:"action"`      // "alert" (default) | "block" — block writes a GuardrailEventRow
	CreatedAt   int64  `json:"created_at"`
}

// CustomMetricResult is a single evaluation of a custom metric against a span.
type CustomMetricResult struct {
	ResultID    string   `json:"result_id"`
	MetricID    string   `json:"metric_id"`
	MetricName  string   `json:"metric_name"`
	SpanID      string   `json:"span_id"`
	TraceID     string   `json:"trace_id"`
	ValueBool   *bool    `json:"value_bool,omitempty"`  // set when output_type=boolean
	ValueScore  float64  `json:"value_score"`            // set when output_type=score
	Reasoning   string   `json:"reasoning"`
	EvaluatedAt int64    `json:"evaluated_at"`
}

// EvalFeedback records a human correction to an evaluation result (Autotune).
type EvalFeedback struct {
	FeedbackID     string   `json:"feedback_id"`
	SpanID         string   `json:"span_id"`
	MetricName     string   `json:"metric_name"`     // standard dim or custom metric name
	OriginalValue  float64  `json:"original_value"`
	CorrectedValue *float64 `json:"corrected_value"` // nil = metric doesn't apply
	Rationale      string   `json:"rationale"`
	CreatedAt      int64    `json:"created_at"`
}

// Custom metrics backend methods are defined in the Backend interface in backend.go.
// This file only holds the types and the Storage pass-through methods.

func (s *Storage) CreateCustomMetric(ctx context.Context, m CustomMetricDef) error {
	return s.backend.CreateCustomMetric(ctx, m)
}

func (s *Storage) ListCustomMetrics(ctx context.Context) ([]CustomMetricDef, error) {
	return s.backend.ListCustomMetrics(ctx)
}

func (s *Storage) SaveCustomMetricResult(ctx context.Context, r CustomMetricResult) error {
	return s.backend.SaveCustomMetricResult(ctx, r)
}

func (s *Storage) QueryCustomMetricResults(ctx context.Context, spanID, metricID string, limit int) ([]CustomMetricResult, error) {
	return s.backend.QueryCustomMetricResults(ctx, spanID, metricID, limit)
}

func (s *Storage) SaveEvalFeedback(ctx context.Context, f EvalFeedback) error {
	return s.backend.SaveEvalFeedback(ctx, f)
}

func (s *Storage) QueryEvalFeedback(ctx context.Context, spanID string) ([]EvalFeedback, error) {
	return s.backend.QueryEvalFeedback(ctx, spanID)
}
