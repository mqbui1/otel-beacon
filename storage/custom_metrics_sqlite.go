package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (b *SQLiteBackend) CreateCustomMetric(ctx context.Context, m CustomMetricDef) error {
	if m.CreatedAt == 0 {
		m.CreatedAt = time.Now().UnixNano()
	}
	_, err := b.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO custom_metrics
		(metric_id, name, description, prompt, output_type, apply_to, action, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		m.MetricID, m.Name, m.Description, m.Prompt, m.OutputType, m.ApplyTo, m.Action, m.CreatedAt)
	return err
}

func (b *SQLiteBackend) ListCustomMetrics(ctx context.Context) ([]CustomMetricDef, error) {
	rows, err := b.db.QueryContext(ctx,
		`SELECT metric_id, name, description, prompt, output_type, apply_to, action, created_at
		 FROM custom_metrics ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []CustomMetricDef
	for rows.Next() {
		var m CustomMetricDef
		if err := rows.Scan(&m.MetricID, &m.Name, &m.Description, &m.Prompt,
			&m.OutputType, &m.ApplyTo, &m.Action, &m.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (b *SQLiteBackend) SaveCustomMetricResult(ctx context.Context, r CustomMetricResult) error {
	var valueBool *int
	if r.ValueBool != nil {
		v := 0
		if *r.ValueBool {
			v = 1
		}
		valueBool = &v
	}
	if r.EvaluatedAt == 0 {
		r.EvaluatedAt = time.Now().UnixNano()
	}
	_, err := b.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO custom_metric_results
		(result_id, metric_id, metric_name, span_id, trace_id, value_bool, value_score, reasoning, evaluated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.ResultID, r.MetricID, r.MetricName, r.SpanID, r.TraceID,
		valueBool, r.ValueScore, r.Reasoning, r.EvaluatedAt)
	return err
}

func (b *SQLiteBackend) QueryCustomMetricResults(ctx context.Context, spanID, metricID string, limit int) ([]CustomMetricResult, error) {
	q := `SELECT result_id, metric_id, metric_name, span_id, trace_id,
	             value_bool, value_score, reasoning, evaluated_at
	      FROM custom_metric_results WHERE 1=1`
	args := []any{}
	if spanID != "" {
		q += " AND span_id = ?"
		args = append(args, spanID)
	}
	if metricID != "" {
		q += " AND metric_id = ?"
		args = append(args, metricID)
	}
	q += " ORDER BY evaluated_at DESC"
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := b.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []CustomMetricResult
	for rows.Next() {
		var r CustomMetricResult
		var valueBool sql.NullInt64
		if err := rows.Scan(&r.ResultID, &r.MetricID, &r.MetricName, &r.SpanID, &r.TraceID,
			&valueBool, &r.ValueScore, &r.Reasoning, &r.EvaluatedAt); err != nil {
			return nil, err
		}
		if valueBool.Valid {
			v := valueBool.Int64 != 0
			r.ValueBool = &v
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (b *SQLiteBackend) SaveEvalFeedback(ctx context.Context, f EvalFeedback) error {
	if f.CreatedAt == 0 {
		f.CreatedAt = time.Now().UnixNano()
	}
	var corrected sql.NullFloat64
	if f.CorrectedValue != nil {
		corrected = sql.NullFloat64{Float64: *f.CorrectedValue, Valid: true}
	}
	_, err := b.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO eval_feedback
		(feedback_id, span_id, metric_name, original_value, corrected_value, rationale, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		f.FeedbackID, f.SpanID, f.MetricName, f.OriginalValue, corrected, f.Rationale, f.CreatedAt)
	return err
}

func (b *SQLiteBackend) QueryEvalFeedback(ctx context.Context, spanID string) ([]EvalFeedback, error) {
	q := `SELECT feedback_id, span_id, metric_name, original_value, corrected_value, rationale, created_at
	      FROM eval_feedback`
	args := []any{}
	if spanID != "" {
		q += " WHERE span_id = ?"
		args = append(args, spanID)
	}
	q += " ORDER BY created_at DESC LIMIT 500"

	rows, err := b.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []EvalFeedback
	for rows.Next() {
		var f EvalFeedback
		var corrected sql.NullFloat64
		if err := rows.Scan(&f.FeedbackID, &f.SpanID, &f.MetricName,
			&f.OriginalValue, &corrected, &f.Rationale, &f.CreatedAt); err != nil {
			return nil, err
		}
		if corrected.Valid {
			f.CorrectedValue = &corrected.Float64
		}
		out = append(out, f)
	}
	return out, rows.Err()
}
