package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

// ClickHouseBackend implements Backend using ClickHouse's MergeTree engine.
// TTL is managed by the table definition — DeleteBefore is a no-op.
//
// DSN format: clickhouse://localhost:9000/otel?username=default&password=
type ClickHouseBackend struct {
	db            *sql.DB
	retentionDays int
}

func NewClickHouseBackend(dsn string, retentionDays int) (*ClickHouseBackend, error) {
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("clickhouse open: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	if retentionDays <= 0 {
		retentionDays = 30
	}
	return &ClickHouseBackend{db: db, retentionDays: retentionDays}, nil
}

func (b *ClickHouseBackend) Init(ctx context.Context) error {
	days := b.retentionDays
	stmts := []string{
		fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS spans (
			entity_id      String,
			trace_id       String,
			span_id        String,
			parent_span_id String,
			name           String,
			kind           Int32,
			start_ns       Int64,
			end_ns         Int64,
			duration_ms    Float64,
			status_code    Int32,
			status_msg     String,
			resource_attrs String,
			span_attrs     String,
			created_at     DateTime DEFAULT now()
		) ENGINE = MergeTree()
		PARTITION BY toYYYYMM(created_at)
		ORDER BY (toDate(created_at), entity_id, trace_id, span_id)
		TTL created_at + INTERVAL %d DAY DELETE`, days),

		fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS metrics (
			entity_id      String,
			name           String,
			description    String,
			unit           String,
			type           String,
			timestamp_ns   Int64,
			value          Float64,
			resource_attrs String,
			data_attrs     String,
			created_at     DateTime DEFAULT now()
		) ENGINE = MergeTree()
		PARTITION BY toYYYYMM(created_at)
		ORDER BY (toDate(created_at), entity_id, name, timestamp_ns)
		TTL created_at + INTERVAL %d DAY DELETE`, days),

		fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS logs (
			entity_id      String,
			timestamp_ns   Int64,
			severity       String,
			body           String,
			trace_id       String,
			span_id        String,
			resource_attrs String,
			log_attrs      String,
			created_at     DateTime DEFAULT now()
		) ENGINE = MergeTree()
		PARTITION BY toYYYYMM(created_at)
		ORDER BY (toDate(created_at), entity_id, timestamp_ns)
		TTL created_at + INTERVAL %d DAY DELETE`, days),

		fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS anomalies (
			metric_name String,
			value       Float64,
			z_score     Float64,
			mean        Float64,
			stddev      Float64,
			algorithm   String,
			detected_at DateTime DEFAULT now()
		) ENGINE = MergeTree()
		ORDER BY (detected_at, metric_name)
		TTL detected_at + INTERVAL %d DAY DELETE`, days),
	}
	for _, s := range stmts {
		if _, err := b.db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("clickhouse init: %w", err)
		}
	}
	return nil
}

func (b *ClickHouseBackend) Close() error { return b.db.Close() }

// ---------------------------------------------------------------------------
// Flush — ClickHouse batch via prepared statement + transaction
// ---------------------------------------------------------------------------

func (b *ClickHouseBackend) FlushSpans(ctx context.Context, batch []SpanRow) error {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO spans (entity_id, trace_id, span_id, parent_span_id, name, kind,
			start_ns, end_ns, duration_ms, status_code, status_msg,
			resource_attrs, span_attrs) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, r := range batch {
		if _, err := stmt.ExecContext(ctx,
			r.EntityID, r.TraceID, r.SpanID, r.ParentSpanID, r.Name, r.Kind,
			r.StartNs, r.EndNs, r.DurationMs, r.StatusCode, r.StatusMsg,
			r.ResourceAttrs, r.SpanAttrs,
		); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (b *ClickHouseBackend) FlushMetrics(ctx context.Context, metrics []MetricRow, anomalies []AnomalyRow) error {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	mstmt, err := tx.PrepareContext(ctx,
		`INSERT INTO metrics (entity_id, name, description, unit, type, timestamp_ns, value, resource_attrs, data_attrs)
		 VALUES (?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer mstmt.Close()
	for _, r := range metrics {
		if _, err := mstmt.ExecContext(ctx,
			r.EntityID, r.Name, r.Description, r.Unit, r.Type,
			r.TimestampNs, r.Value, r.ResourceAttrs, r.DataAttrs,
		); err != nil {
			tx.Rollback()
			return err
		}
	}
	if len(anomalies) > 0 {
		astmt, err := tx.PrepareContext(ctx,
			`INSERT INTO anomalies (metric_name, value, z_score, mean, stddev, algorithm) VALUES (?,?,?,?,?,?)`)
		if err != nil {
			tx.Rollback()
			return err
		}
		defer astmt.Close()
		for _, a := range anomalies {
			if _, err := astmt.ExecContext(ctx, a.MetricName, a.Value, a.Score, a.Mean, a.StdDev, a.Algorithm); err != nil {
				tx.Rollback()
				return err
			}
		}
	}
	return tx.Commit()
}

func (b *ClickHouseBackend) FlushLogs(ctx context.Context, batch []LogRow) error {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO logs (entity_id, timestamp_ns, severity, body, trace_id, span_id, resource_attrs, log_attrs)
		 VALUES (?,?,?,?,?,?,?,?)`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, r := range batch {
		if _, err := stmt.ExecContext(ctx,
			r.EntityID, r.TimestampNs, r.Severity, r.Body, r.TraceID, r.SpanID,
			r.ResourceAttrs, r.LogAttrs,
		); err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

func (b *ClickHouseBackend) QuerySpans(ctx context.Context, q SpanQuery) ([]SpanRow, error) {
	where, args := chSpanWhere(q)
	rows, err := b.db.QueryContext(ctx,
		`SELECT entity_id, trace_id, span_id, parent_span_id, name, kind,
			start_ns, end_ns, duration_ms, status_code, status_msg,
			resource_attrs, span_attrs
		 FROM spans`+where+` ORDER BY start_ns DESC LIMIT ?`,
		append(args, limit(q.Limit))...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanSpans(rows)
}

func (b *ClickHouseBackend) QueryMetrics(ctx context.Context, q MetricQuery) ([]MetricRow, error) {
	where, args := chMetricWhere(q)
	rows, err := b.db.QueryContext(ctx,
		`SELECT entity_id, name, description, unit, type, timestamp_ns, value, resource_attrs, data_attrs
		 FROM metrics`+where+` ORDER BY timestamp_ns DESC LIMIT ?`,
		append(args, limit(q.Limit))...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanMetrics(rows)
}

func (b *ClickHouseBackend) QueryLogs(ctx context.Context, q LogQuery) ([]LogRow, error) {
	where, args := chLogWhere(q)
	rows, err := b.db.QueryContext(ctx,
		`SELECT entity_id, timestamp_ns, severity, body, trace_id, span_id, resource_attrs, log_attrs
		 FROM logs`+where+` ORDER BY timestamp_ns DESC LIMIT ?`,
		append(args, limit(q.Limit))...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLogs(rows)
}

func (b *ClickHouseBackend) QueryAnomalies(ctx context.Context, entityID string, lim int) ([]AnomalyRow, error) {
	var q string
	var args []any
	if entityID != "" {
		q = `SELECT entity_id, signal_type, detector_name, metric_name,
		            value, z_score, mean, stddev, algorithm, severity, description,
		            toUnixTimestamp(detected_at)
		     FROM anomalies WHERE entity_id = ? ORDER BY detected_at DESC LIMIT ?`
		args = []any{entityID, limit(lim)}
	} else {
		q = `SELECT entity_id, signal_type, detector_name, metric_name,
		            value, z_score, mean, stddev, algorithm, severity, description,
		            toUnixTimestamp(detected_at)
		     FROM anomalies ORDER BY detected_at DESC LIMIT ?`
		args = []any{limit(lim)}
	}
	rows, err := b.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AnomalyRow
	for rows.Next() {
		var r AnomalyRow
		if err := rows.Scan(
			&r.EntityID, &r.SignalType, &r.DetectorName, &r.MetricName,
			&r.Value, &r.Score, &r.Mean, &r.StdDev, &r.Algorithm, &r.Severity, &r.Description, &r.DetectedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// DeleteBefore is a no-op for ClickHouse — TTL handles retention automatically.
func (b *ClickHouseBackend) DeleteBefore(_ context.Context, _ int64) error { return nil }

// Entity / topology — no-op stubs for ClickHouse (SQLite-only feature for now).
func (b *ClickHouseBackend) UpsertEntities(_ context.Context, _ []EntityRow) error { return nil }
func (b *ClickHouseBackend) RefreshTopology(_ context.Context) error                { return nil }
func (b *ClickHouseBackend) QueryEntities(_ context.Context, _, _ string) ([]EntityRow, error) {
	return nil, nil
}
func (b *ClickHouseBackend) QueryEnvironments(_ context.Context) ([]string, error) {
	return nil, nil
}
func (b *ClickHouseBackend) QueryTopology(_ context.Context) ([]TopologyEdge, error) {
	return nil, nil
}

// Fingerprint / error signature — stubs for ClickHouse (SQLite-only for now).
func (b *ClickHouseBackend) FlushAnomalies(_ context.Context, _ []AnomalyRow) error { return nil }
func (b *ClickHouseBackend) DeleteMissingServiceAnomaly(_ context.Context, _ string) error {
	return nil
}
func (b *ClickHouseBackend) UpsertTraceFingerprint(_ context.Context, _ TraceFingerprintRow) error {
	return nil
}
func (b *ClickHouseBackend) QueryTraceFingerprints(_ context.Context, _ string) ([]TraceFingerprintRow, error) {
	return nil, nil
}
func (b *ClickHouseBackend) UpsertErrorSignature(_ context.Context, _ ErrorSignatureRow) error {
	return nil
}
func (b *ClickHouseBackend) QueryErrorSignatures(_ context.Context, _ string) ([]ErrorSignatureRow, error) {
	return nil, nil
}

// ---------------------------------------------------------------------------
// ClickHouse WHERE clause helpers (uses ? placeholders same as SQLite)
// ---------------------------------------------------------------------------

func chSpanWhere(q SpanQuery) (string, []any) {
	var c []string
	var a []any
	if q.TraceID != "" {
		c = append(c, "trace_id = ?")
		a = append(a, q.TraceID)
	}
	if q.Name != "" {
		c = append(c, "name = ?")
		a = append(a, q.Name)
	}
	if q.Service != "" {
		// Fast path: entity_id column (in ORDER BY — efficient). Legacy fallback for old rows.
		c = append(c, "(entity_id = ? OR (entity_id = '' AND JSONExtractString(resource_attrs, 'service.name') = ?))")
		a = append(a, q.Service, q.Service)
	}
	if q.From > 0 {
		c = append(c, "start_ns >= ?")
		a = append(a, q.From)
	}
	if q.To > 0 {
		c = append(c, "start_ns <= ?")
		a = append(a, q.To)
	}
	return whereClause(c), a
}

func chMetricWhere(q MetricQuery) (string, []any) {
	var c []string
	var a []any
	if q.Name != "" {
		c = append(c, "name = ?")
		a = append(a, q.Name)
	}
	if len(q.NamePrefixes) > 0 {
		parts := make([]string, len(q.NamePrefixes))
		for i, p := range q.NamePrefixes {
			parts[i] = "name LIKE ?"
			a = append(a, p+".%")
		}
		c = append(c, "("+strings.Join(parts, " OR ")+")")
	}
	if q.Service != "" || len(q.K8sAttrs) > 0 {
		sub, subArgs := chServiceOrK8sClauses(q.Service, q.K8sAttrs)
		c = append(c, sub)
		a = append(a, subArgs...)
	}
	if q.From > 0 {
		c = append(c, "timestamp_ns >= ?")
		a = append(a, q.From)
	}
	if q.To > 0 {
		c = append(c, "timestamp_ns <= ?")
		a = append(a, q.To)
	}
	return whereClause(c), a
}

func chLogWhere(q LogQuery) (string, []any) {
	var c []string
	var a []any
	if q.Severity != "" {
		c = append(c, "severity = ?")
		a = append(a, q.Severity)
	}
	if q.TraceID != "" {
		c = append(c, "trace_id = ?")
		a = append(a, q.TraceID)
	}
	if q.Service != "" || len(q.K8sAttrs) > 0 {
		sub, subArgs := chServiceOrK8sClauses(q.Service, q.K8sAttrs)
		c = append(c, sub)
		a = append(a, subArgs...)
	}
	if q.From > 0 {
		c = append(c, "timestamp_ns >= ?")
		a = append(a, q.From)
	}
	if q.To > 0 {
		c = append(c, "timestamp_ns <= ?")
		a = append(a, q.To)
	}
	return whereClause(c), a
}

func chServiceOrK8sClauses(service string, k8sAttrs map[string]string) (string, []any) {
	var parts []string
	var args []any
	if service != "" {
		// Fast path: entity_id column (stored in the ORDER BY key — very efficient).
		parts = append(parts, "entity_id = ?")
		args = append(args, service)
		// Legacy fallback: rows without entity_id (empty string in ClickHouse).
		parts = append(parts, "(entity_id = '' AND JSONExtractString(resource_attrs, 'service.name') = ?)")
		args = append(args, service)
	}
	allowed := map[string]bool{
		"k8s.pod.name":        true,
		"k8s.deployment.name": true,
		"k8s.namespace.name":  true,
		"k8s.node.name":       true,
		"k8s.container.name":  true,
	}
	for k, v := range k8sAttrs {
		if !allowed[k] {
			continue
		}
		parts = append(parts, "(entity_id = '' AND JSONExtractString(resource_attrs, '"+k+"') = ?)")
		args = append(args, v)
	}
	if len(parts) == 0 {
		return "1=1", nil
	}
	return "(" + strings.Join(parts, " OR ") + ")", args
}

// shared WHERE helper already defined in sqlite.go — using strings pkg here
var _ = strings.Join // prevent unused import

// ---------------------------------------------------------------------------
// Shared scan helpers (used by both backends via package-level functions)
// ---------------------------------------------------------------------------

func scanSpans(rows *sql.Rows) ([]SpanRow, error) {
	var out []SpanRow
	for rows.Next() {
		var r SpanRow
		if err := rows.Scan(&r.EntityID, &r.TraceID, &r.SpanID, &r.ParentSpanID, &r.Name, &r.Kind,
			&r.StartNs, &r.EndNs, &r.DurationMs, &r.StatusCode, &r.StatusMsg,
			&r.ResourceAttrs, &r.SpanAttrs,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func scanMetrics(rows *sql.Rows) ([]MetricRow, error) {
	var out []MetricRow
	for rows.Next() {
		var r MetricRow
		if err := rows.Scan(&r.EntityID, &r.Name, &r.Description, &r.Unit, &r.Type,
			&r.TimestampNs, &r.Value, &r.ResourceAttrs, &r.DataAttrs,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func scanLogs(rows *sql.Rows) ([]LogRow, error) {
	var out []LogRow
	for rows.Next() {
		var r LogRow
		if err := rows.Scan(&r.EntityID, &r.TimestampNs, &r.Severity, &r.Body,
			&r.TraceID, &r.SpanID, &r.ResourceAttrs, &r.LogAttrs,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// GenAI — ClickHouse stubs (not yet implemented; SQLite is primary for GenAI)
// ---------------------------------------------------------------------------

func (b *ClickHouseBackend) FlushGenAISpans(_ context.Context, _ []GenAISpanRow) error {
	return nil
}
func (b *ClickHouseBackend) FlushEvalResults(_ context.Context, _ []EvalResultRow) error {
	return nil
}
func (b *ClickHouseBackend) FlushGuardrailEvents(_ context.Context, _ []GuardrailEventRow) error {
	return nil
}
func (b *ClickHouseBackend) QueryGenAISpans(_ context.Context, _ GenAIQuery) ([]GenAISpanRow, error) {
	return nil, nil
}
func (b *ClickHouseBackend) QueryGenAIAgents(_ context.Context, _, _ int64) ([]GenAIAgentRow, error) {
	return nil, nil
}
func (b *ClickHouseBackend) QueryGenAICosts(_ context.Context, _, _ int64, _ string) ([]GenAICostSummary, error) {
	return nil, nil
}
func (b *ClickHouseBackend) QueryEvalResults(_ context.Context, _ string, _ int) ([]EvalResultRow, error) {
	return nil, nil
}
func (b *ClickHouseBackend) QueryGuardrailEvents(_ context.Context, _ string, _ int) ([]GuardrailEventRow, error) {
	return nil, nil
}
func (b *ClickHouseBackend) FlushSessions(_ context.Context, _ []string) ([]SessionRow, error) {
	return nil, nil
}
func (b *ClickHouseBackend) FlushSessionEval(_ context.Context, _ SessionRow) error { return nil }
func (b *ClickHouseBackend) QuerySessions(_ context.Context, _ string, _ int) ([]SessionRow, error) {
	return nil, nil
}
func (b *ClickHouseBackend) QuerySession(_ context.Context, _ string) (*SessionRow, error) {
	return nil, nil
}

// Datasets + Experiments — no-op stubs for ClickHouse.
func (b *ClickHouseBackend) CreateDataset(_ context.Context, _ DatasetMeta, _ []DatasetRow) error {
	return nil
}
func (b *ClickHouseBackend) ListDatasets(_ context.Context, _ string, _ int) ([]DatasetMeta, error) {
	return nil, nil
}
func (b *ClickHouseBackend) GetDataset(_ context.Context, _ string) (*DatasetMeta, []DatasetRow, error) {
	return nil, nil, nil
}
func (b *ClickHouseBackend) CreateExperiment(_ context.Context, _ ExperimentRow) error { return nil }
func (b *ClickHouseBackend) ListExperiments(_ context.Context, _ string, _ int) ([]ExperimentRow, error) {
	return nil, nil
}
func (b *ClickHouseBackend) GetExperiment(_ context.Context, _ string) (*ExperimentRow, error) {
	return nil, nil
}
func (b *ClickHouseBackend) SaveExperimentResult(_ context.Context, _ ExperimentResult) error {
	return nil
}
func (b *ClickHouseBackend) FinalizeExperiment(_ context.Context, _ ExperimentRow) error { return nil }
func (b *ClickHouseBackend) GetExperimentResults(_ context.Context, _ string) ([]ExperimentResult, error) {
	return nil, nil
}
func (b *ClickHouseBackend) CreateCustomMetric(_ context.Context, _ CustomMetricDef) error {
	return nil
}
func (b *ClickHouseBackend) ListCustomMetrics(_ context.Context) ([]CustomMetricDef, error) {
	return nil, nil
}
func (b *ClickHouseBackend) SaveCustomMetricResult(_ context.Context, _ CustomMetricResult) error {
	return nil
}
func (b *ClickHouseBackend) QueryCustomMetricResults(_ context.Context, _, _ string, _ int) ([]CustomMetricResult, error) {
	return nil, nil
}
func (b *ClickHouseBackend) SaveEvalFeedback(_ context.Context, _ EvalFeedback) error { return nil }
func (b *ClickHouseBackend) QueryEvalFeedback(_ context.Context, _ string) ([]EvalFeedback, error) {
	return nil, nil
}
