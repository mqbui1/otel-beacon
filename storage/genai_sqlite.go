package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// ---------------------------------------------------------------------------
// SQLite implementation of GenAI backend methods
// ---------------------------------------------------------------------------

func (b *SQLiteBackend) FlushGenAISpans(ctx context.Context, batch []GenAISpanRow) error {
	return b.inTx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO genai_spans
				(trace_id, span_id, parent_span_id, entity_id,
				 system, operation, model, agent_name, tool_name,
				 input_tokens, output_tokens, total_cost_usd,
				 start_ns, duration_ms, status_code,
				 prompt, completion, span_attrs, resource_attrs)
			VALUES (?,?,?,?, ?,?,?,?,?, ?,?,?, ?,?,?, ?,?,?,?)`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, r := range batch {
			if _, err := stmt.ExecContext(ctx,
				r.TraceID, r.SpanID, r.ParentSpanID, r.EntityID,
				r.System, r.Operation, r.Model, r.AgentName, r.ToolName,
				r.InputTokens, r.OutputTokens, r.TotalCostUSD,
				r.StartNs, r.DurationMs, r.StatusCode,
				r.Prompt, r.Completion, r.SpanAttrs, r.ResourceAttrs,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *SQLiteBackend) FlushEvalResults(ctx context.Context, batch []EvalResultRow) error {
	return b.inTx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT OR REPLACE INTO eval_results
				(span_id, trace_id, hallucination, coherence, relevance, toxicity, overall_score, reasoning, evaluated_at)
			VALUES (?,?,?,?,?,?,?,?,?)`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, r := range batch {
			if _, err := stmt.ExecContext(ctx,
				r.SpanID, r.TraceID,
				r.Hallucination, r.Coherence, r.Relevance, r.Toxicity,
				r.OverallScore, r.Reasoning, r.EvaluatedAt,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *SQLiteBackend) FlushGuardrailEvents(ctx context.Context, batch []GuardrailEventRow) error {
	return b.inTx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO guardrail_events
				(span_id, trace_id, check_type, triggered, severity, detail, checked_at)
			VALUES (?,?,?,?,?,?,?)`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, r := range batch {
			triggered := 0
			if r.Triggered {
				triggered = 1
			}
			if _, err := stmt.ExecContext(ctx,
				r.SpanID, r.TraceID, r.CheckType, triggered, r.Severity, r.Detail, r.CheckedAt,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *SQLiteBackend) QueryGenAISpans(ctx context.Context, q GenAIQuery) ([]GenAISpanRow, error) {
	where, args := genaiWhere(q)
	lim := limit(q.Limit)
	rows, err := b.db.QueryContext(ctx,
		`SELECT trace_id, span_id, parent_span_id, entity_id,
			system, operation, model, agent_name, tool_name,
			input_tokens, output_tokens, total_cost_usd,
			start_ns, duration_ms, status_code,
			prompt, completion, span_attrs, resource_attrs
		 FROM genai_spans`+where+` ORDER BY start_ns DESC LIMIT ?`,
		append(args, lim)...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GenAISpanRow
	for rows.Next() {
		var r GenAISpanRow
		if err := rows.Scan(
			&r.TraceID, &r.SpanID, &r.ParentSpanID, &r.EntityID,
			&r.System, &r.Operation, &r.Model, &r.AgentName, &r.ToolName,
			&r.InputTokens, &r.OutputTokens, &r.TotalCostUSD,
			&r.StartNs, &r.DurationMs, &r.StatusCode,
			&r.Prompt, &r.Completion, &r.SpanAttrs, &r.ResourceAttrs,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (b *SQLiteBackend) QueryGenAIAgents(ctx context.Context, from, to int64) ([]GenAIAgentRow, error) {
	where, args := timeRange("start_ns", from, to)
	rows, err := b.db.QueryContext(ctx,
		`SELECT
			COALESCE(NULLIF(agent_name,''), entity_id) AS agent,
			entity_id,
			COUNT(*) AS span_count,
			SUM(total_cost_usd) AS total_cost,
			SUM(input_tokens + output_tokens) AS total_tokens,
			AVG(duration_ms) AS avg_dur,
			CAST(SUM(CASE WHEN status_code = 2 THEN 1 ELSE 0 END) AS REAL) / COUNT(*) AS error_rate,
			MAX(start_ns) AS last_seen
		 FROM genai_spans`+where+`
		 GROUP BY agent, entity_id
		 ORDER BY total_cost DESC LIMIT 200`,
		args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GenAIAgentRow
	for rows.Next() {
		var r GenAIAgentRow
		if err := rows.Scan(
			&r.AgentName, &r.EntityID, &r.SpanCount,
			&r.TotalCostUSD, &r.TotalTokens, &r.AvgDurationMs,
			&r.ErrorRate, &r.LastSeenAt,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (b *SQLiteBackend) QueryGenAICosts(ctx context.Context, from, to int64, groupBy string) ([]GenAICostSummary, error) {
	// Whitelist groupBy to prevent injection.
	var col string
	switch groupBy {
	case "agent":
		col = "COALESCE(NULLIF(agent_name,''), entity_id)"
	case "service":
		col = "entity_id"
	default:
		col = "model"
		groupBy = "model"
	}
	where, args := timeRange("start_ns", from, to)
	query := fmt.Sprintf(`
		SELECT %s AS dim, COUNT(*), SUM(total_cost_usd), SUM(input_tokens+output_tokens)
		FROM genai_spans%s
		GROUP BY dim ORDER BY SUM(total_cost_usd) DESC LIMIT 100`, col, where)

	rows, err := b.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GenAICostSummary
	for rows.Next() {
		var r GenAICostSummary
		r.GroupBy = groupBy
		if err := rows.Scan(&r.Dimension, &r.SpanCount, &r.TotalCostUSD, &r.TotalTokens); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (b *SQLiteBackend) QueryEvalResults(ctx context.Context, traceID string, lim int) ([]EvalResultRow, error) {
	var where string
	var args []any
	if traceID != "" {
		where = " WHERE trace_id = ?"
		args = append(args, traceID)
	}
	rows, err := b.db.QueryContext(ctx,
		`SELECT span_id, trace_id, hallucination, coherence, relevance, toxicity, overall_score, reasoning, evaluated_at
		 FROM eval_results`+where+` ORDER BY evaluated_at DESC LIMIT ?`,
		append(args, limit(lim))...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EvalResultRow
	for rows.Next() {
		var r EvalResultRow
		if err := rows.Scan(
			&r.SpanID, &r.TraceID,
			&r.Hallucination, &r.Coherence, &r.Relevance, &r.Toxicity,
			&r.OverallScore, &r.Reasoning, &r.EvaluatedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (b *SQLiteBackend) QueryGuardrailEvents(ctx context.Context, traceID string, lim int) ([]GuardrailEventRow, error) {
	var where string
	var args []any
	if traceID != "" {
		where = " WHERE trace_id = ?"
		args = append(args, traceID)
	} else {
		where = " WHERE triggered = 1"
	}
	rows, err := b.db.QueryContext(ctx,
		`SELECT span_id, trace_id, check_type, triggered, severity, detail, checked_at
		 FROM guardrail_events`+where+` ORDER BY checked_at DESC LIMIT ?`,
		append(args, limit(lim))...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GuardrailEventRow
	for rows.Next() {
		var r GuardrailEventRow
		var triggered int
		if err := rows.Scan(
			&r.SpanID, &r.TraceID, &r.CheckType, &triggered, &r.Severity, &r.Detail, &r.CheckedAt,
		); err != nil {
			return nil, err
		}
		r.Triggered = triggered != 0
		out = append(out, r)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func genaiWhere(q GenAIQuery) (string, []any) {
	var clauses []string
	var args []any
	if q.TraceID != "" {
		clauses = append(clauses, "trace_id = ?")
		args = append(args, q.TraceID)
	}
	if q.AgentName != "" {
		clauses = append(clauses, "agent_name = ?")
		args = append(args, q.AgentName)
	}
	if q.Model != "" {
		clauses = append(clauses, "model = ?")
		args = append(args, q.Model)
	}
	if q.System != "" {
		clauses = append(clauses, "system = ?")
		args = append(args, q.System)
	}
	if q.From > 0 {
		clauses = append(clauses, "start_ns >= ?")
		args = append(args, q.From)
	}
	if q.To > 0 {
		clauses = append(clauses, "start_ns <= ?")
		args = append(args, q.To)
	}
	if len(clauses) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(clauses, " AND "), args
}

func timeRange(col string, from, to int64) (string, []any) {
	var clauses []string
	var args []any
	if from > 0 {
		clauses = append(clauses, col+" >= ?")
		args = append(args, from)
	}
	if to > 0 {
		clauses = append(clauses, col+" <= ?")
		args = append(args, to)
	}
	if len(clauses) == 0 {
		return "", args
	}
	return " WHERE " + strings.Join(clauses, " AND "), args
}
