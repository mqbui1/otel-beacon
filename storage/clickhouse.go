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
		ORDER BY (toDate(created_at), trace_id, span_id)
		TTL created_at + INTERVAL %d DAY DELETE`, days),

		fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS metrics (
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
		ORDER BY (toDate(created_at), name, timestamp_ns)
		TTL created_at + INTERVAL %d DAY DELETE`, days),

		fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS logs (
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
		ORDER BY (toDate(created_at), timestamp_ns)
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
		`INSERT INTO spans (trace_id, span_id, parent_span_id, name, kind,
			start_ns, end_ns, duration_ms, status_code, status_msg,
			resource_attrs, span_attrs) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, r := range batch {
		if _, err := stmt.ExecContext(ctx,
			r.TraceID, r.SpanID, r.ParentSpanID, r.Name, r.Kind,
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
		`INSERT INTO metrics (name, description, unit, type, timestamp_ns, value, resource_attrs, data_attrs)
		 VALUES (?,?,?,?,?,?,?,?)`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer mstmt.Close()
	for _, r := range metrics {
		if _, err := mstmt.ExecContext(ctx,
			r.Name, r.Description, r.Unit, r.Type,
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
		`INSERT INTO logs (timestamp_ns, severity, body, trace_id, span_id, resource_attrs, log_attrs)
		 VALUES (?,?,?,?,?,?,?)`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, r := range batch {
		if _, err := stmt.ExecContext(ctx,
			r.TimestampNs, r.Severity, r.Body, r.TraceID, r.SpanID,
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
		`SELECT trace_id, span_id, parent_span_id, name, kind,
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
		`SELECT name, description, unit, type, timestamp_ns, value, resource_attrs, data_attrs
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
		`SELECT timestamp_ns, severity, body, trace_id, span_id, resource_attrs, log_attrs
		 FROM logs`+where+` ORDER BY timestamp_ns DESC LIMIT ?`,
		append(args, limit(q.Limit))...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanLogs(rows)
}

func (b *ClickHouseBackend) QueryAnomalies(ctx context.Context, lim int) ([]AnomalyRow, error) {
	rows, err := b.db.QueryContext(ctx,
		`SELECT metric_name, value, z_score, mean, stddev, algorithm, toUnixTimestamp(detected_at)
		 FROM anomalies ORDER BY detected_at DESC LIMIT ?`, limit(lim))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AnomalyRow
	for rows.Next() {
		var r AnomalyRow
		if err := rows.Scan(&r.MetricName, &r.Value, &r.Score, &r.Mean, &r.StdDev, &r.Algorithm, &r.DetectedAt); err != nil {
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
func (b *ClickHouseBackend) QueryEntities(_ context.Context, _ string) ([]EntityRow, error) {
	return nil, nil
}
func (b *ClickHouseBackend) QueryTopology(_ context.Context) ([]TopologyEdge, error) {
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
		c = append(c, "JSONExtractString(resource_attrs, 'service.name') = ?")
		a = append(a, q.Service)
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
		parts = append(parts, "JSONExtractString(resource_attrs, 'service.name') = ?")
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
		parts = append(parts, "JSONExtractString(resource_attrs, '"+k+"') = ?")
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
		if err := rows.Scan(&r.TraceID, &r.SpanID, &r.ParentSpanID, &r.Name, &r.Kind,
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
		if err := rows.Scan(&r.Name, &r.Description, &r.Unit, &r.Type,
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
		if err := rows.Scan(&r.TimestampNs, &r.Severity, &r.Body,
			&r.TraceID, &r.SpanID, &r.ResourceAttrs, &r.LogAttrs,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
