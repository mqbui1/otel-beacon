package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "modernc.org/sqlite"
)

type SQLiteBackend struct {
	db *sql.DB
}

func NewSQLiteBackend(dsn string) (*SQLiteBackend, error) {
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite open: %w", err)
	}
	db.SetMaxOpenConns(1) // SQLite: single writer
	db.SetMaxIdleConns(1)
	return &SQLiteBackend{db: db}, nil
}

func (b *SQLiteBackend) Init(ctx context.Context) error {
	if _, err := b.db.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		return err
	}
	_, err := b.db.ExecContext(ctx, sqliteSchema)
	return err
}

func (b *SQLiteBackend) Close() error { return b.db.Close() }

// ---------------------------------------------------------------------------
// Flush (batch insert via prepared statement in a transaction)
// ---------------------------------------------------------------------------

func (b *SQLiteBackend) FlushSpans(ctx context.Context, batch []SpanRow) error {
	return b.inTx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO spans
				(trace_id, span_id, parent_span_id, name, kind,
				 start_ns, end_ns, duration_ms, status_code, status_msg,
				 resource_attrs, span_attrs)
			VALUES (?,?,?,?,?, ?,?,?,?,?, ?,?)`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, r := range batch {
			if _, err := stmt.ExecContext(ctx,
				r.TraceID, r.SpanID, r.ParentSpanID, r.Name, r.Kind,
				r.StartNs, r.EndNs, r.DurationMs, r.StatusCode, r.StatusMsg,
				r.ResourceAttrs, r.SpanAttrs,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *SQLiteBackend) FlushMetrics(ctx context.Context, metrics []MetricRow, anomalies []AnomalyRow) error {
	return b.inTx(ctx, func(tx *sql.Tx) error {
		mstmt, err := tx.PrepareContext(ctx, `
			INSERT INTO metrics
				(name, description, unit, type, timestamp_ns, value, resource_attrs, data_attrs)
			VALUES (?,?,?,?,?,?,?,?)`)
		if err != nil {
			return err
		}
		defer mstmt.Close()
		for _, r := range metrics {
			if _, err := mstmt.ExecContext(ctx,
				r.Name, r.Description, r.Unit, r.Type,
				r.TimestampNs, r.Value, r.ResourceAttrs, r.DataAttrs,
			); err != nil {
				return err
			}
		}
		if len(anomalies) == 0 {
			return nil
		}
		astmt, err := tx.PrepareContext(ctx, `
			INSERT INTO anomalies (metric_name, value, z_score, mean, stddev, algorithm, detected_at)
			VALUES (?,?,?,?,?,?,?)`)
		if err != nil {
			return err
		}
		defer astmt.Close()
		for _, a := range anomalies {
			if _, err := astmt.ExecContext(ctx,
				a.MetricName, a.Value, a.Score, a.Mean, a.StdDev, a.Algorithm, a.DetectedAt,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *SQLiteBackend) FlushLogs(ctx context.Context, batch []LogRow) error {
	return b.inTx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO logs
				(timestamp_ns, severity, body, trace_id, span_id, resource_attrs, log_attrs)
			VALUES (?,?,?,?,?,?,?)`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, r := range batch {
			if _, err := stmt.ExecContext(ctx,
				r.TimestampNs, r.Severity, r.Body, r.TraceID, r.SpanID,
				r.ResourceAttrs, r.LogAttrs,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

func (b *SQLiteBackend) QuerySpans(ctx context.Context, q SpanQuery) ([]SpanRow, error) {
	where, args := spanWhere(q)
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

func (b *SQLiteBackend) QueryMetrics(ctx context.Context, q MetricQuery) ([]MetricRow, error) {
	where, args := metricWhere(q)
	rows, err := b.db.QueryContext(ctx,
		`SELECT name, description, unit, type, timestamp_ns, value, resource_attrs, data_attrs
		 FROM metrics`+where+` ORDER BY timestamp_ns DESC LIMIT ?`,
		append(args, limit(q.Limit))...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func (b *SQLiteBackend) QueryLogs(ctx context.Context, q LogQuery) ([]LogRow, error) {
	where, args := logWhere(q)
	rows, err := b.db.QueryContext(ctx,
		`SELECT timestamp_ns, severity, body, trace_id, span_id, resource_attrs, log_attrs
		 FROM logs`+where+` ORDER BY timestamp_ns DESC LIMIT ?`,
		append(args, limit(q.Limit))...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func (b *SQLiteBackend) QueryAnomalies(ctx context.Context, lim int) ([]AnomalyRow, error) {
	rows, err := b.db.QueryContext(ctx,
		`SELECT metric_name, value, z_score, mean, stddev, algorithm, detected_at
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

func (b *SQLiteBackend) DeleteBefore(ctx context.Context, cutoff int64) error {
	for _, q := range []string{
		`DELETE FROM spans WHERE created_at < ?`,
		`DELETE FROM metrics WHERE created_at < ?`,
		`DELETE FROM logs WHERE created_at < ?`,
		`DELETE FROM anomalies WHERE detected_at < ?`,
	} {
		if _, err := b.db.ExecContext(ctx, q, cutoff); err != nil {
			return err
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (b *SQLiteBackend) inTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func spanWhere(q SpanQuery) (string, []any) {
	var clauses []string
	var args []any
	if q.TraceID != "" {
		clauses = append(clauses, "trace_id = ?")
		args = append(args, q.TraceID)
	}
	if q.Name != "" {
		clauses = append(clauses, "name = ?")
		args = append(args, q.Name)
	}
	if q.Service != "" {
		clauses = append(clauses, `json_extract(resource_attrs, '$."service.name"') = ?`)
		args = append(args, q.Service)
	}
	if q.From > 0 {
		clauses = append(clauses, "start_ns >= ?")
		args = append(args, q.From)
	}
	if q.To > 0 {
		clauses = append(clauses, "start_ns <= ?")
		args = append(args, q.To)
	}
	return whereClause(clauses), args
}

func metricWhere(q MetricQuery) (string, []any) {
	var clauses []string
	var args []any
	if q.Name != "" {
		clauses = append(clauses, "name = ?")
		args = append(args, q.Name)
	}
	if q.Service != "" {
		clauses = append(clauses, `json_extract(resource_attrs, '$."service.name"') = ?`)
		args = append(args, q.Service)
	}
	if q.From > 0 {
		clauses = append(clauses, "timestamp_ns >= ?")
		args = append(args, q.From)
	}
	if q.To > 0 {
		clauses = append(clauses, "timestamp_ns <= ?")
		args = append(args, q.To)
	}
	return whereClause(clauses), args
}

func logWhere(q LogQuery) (string, []any) {
	var clauses []string
	var args []any
	if q.Severity != "" {
		clauses = append(clauses, "severity = ?")
		args = append(args, q.Severity)
	}
	if q.TraceID != "" {
		clauses = append(clauses, "trace_id = ?")
		args = append(args, q.TraceID)
	}
	if q.Service != "" {
		clauses = append(clauses, `json_extract(resource_attrs, '$."service.name"') = ?`)
		args = append(args, q.Service)
	}
	if q.From > 0 {
		clauses = append(clauses, "timestamp_ns >= ?")
		args = append(args, q.From)
	}
	if q.To > 0 {
		clauses = append(clauses, "timestamp_ns <= ?")
		args = append(args, q.To)
	}
	return whereClause(clauses), args
}

func whereClause(clauses []string) string {
	if len(clauses) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(clauses, " AND ")
}

func limit(n int) int {
	if n <= 0 || n > 1000 {
		return 100
	}
	return n
}

// ---------------------------------------------------------------------------
// Entity / topology
// ---------------------------------------------------------------------------

func (b *SQLiteBackend) UpsertEntities(ctx context.Context, entities []EntityRow) error {
	return b.inTx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO entities (entity_type, entity_id, attrs, last_seen_ns)
			VALUES (?, ?, ?, ?)
			ON CONFLICT(entity_type, entity_id) DO UPDATE SET
				attrs        = excluded.attrs,
				last_seen_ns = MAX(entities.last_seen_ns, excluded.last_seen_ns)`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, e := range entities {
			if _, err := stmt.ExecContext(ctx, e.EntityType, e.EntityID, e.Attrs, e.LastSeenNs); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *SQLiteBackend) RefreshTopology(ctx context.Context) error {
	_, err := b.db.ExecContext(ctx, `
		INSERT OR REPLACE INTO service_topology
			(source_service, target_service, call_count, error_count, avg_duration_ms, updated_at)
		SELECT
			json_extract(parent.resource_attrs, '$."service.name"') AS source_service,
			json_extract(child.resource_attrs,  '$."service.name"') AS target_service,
			COUNT(*)                                                 AS call_count,
			SUM(CASE WHEN child.status_code = 2 THEN 1 ELSE 0 END)  AS error_count,
			AVG(child.duration_ms)                                   AS avg_duration_ms,
			unixepoch()                                              AS updated_at
		FROM spans child
		JOIN spans parent ON child.parent_span_id = parent.span_id
		WHERE child.start_ns > (unixepoch() - 3600) * 1000000000
		  AND json_extract(parent.resource_attrs, '$."service.name"') IS NOT NULL
		  AND json_extract(child.resource_attrs,  '$."service.name"') IS NOT NULL
		  AND json_extract(parent.resource_attrs, '$."service.name"')
		   != json_extract(child.resource_attrs,  '$."service.name"')
		GROUP BY source_service, target_service`)
	return err
}

func (b *SQLiteBackend) QueryEntities(ctx context.Context, entityType string) ([]EntityRow, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if entityType != "" {
		rows, err = b.db.QueryContext(ctx,
			`SELECT entity_type, entity_id, attrs, last_seen_ns FROM entities
			 WHERE entity_type = ? ORDER BY last_seen_ns DESC`, entityType)
	} else {
		rows, err = b.db.QueryContext(ctx,
			`SELECT entity_type, entity_id, attrs, last_seen_ns FROM entities
			 ORDER BY entity_type, last_seen_ns DESC`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EntityRow
	for rows.Next() {
		var r EntityRow
		if err := rows.Scan(&r.EntityType, &r.EntityID, &r.Attrs, &r.LastSeenNs); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (b *SQLiteBackend) QueryTopology(ctx context.Context) ([]TopologyEdge, error) {
	rows, err := b.db.QueryContext(ctx,
		`SELECT source_service, target_service, call_count, error_count, avg_duration_ms, updated_at
		 FROM service_topology ORDER BY call_count DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TopologyEdge
	for rows.Next() {
		var e TopologyEdge
		if err := rows.Scan(&e.SourceService, &e.TargetService, &e.CallCount, &e.ErrorCount, &e.AvgDurationMs, &e.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

const sqliteSchema = `
CREATE TABLE IF NOT EXISTS spans (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    trace_id       TEXT    NOT NULL,
    span_id        TEXT    NOT NULL,
    parent_span_id TEXT,
    name           TEXT    NOT NULL,
    kind           INTEGER,
    start_ns       INTEGER,
    end_ns         INTEGER,
    duration_ms    REAL,
    status_code    INTEGER,
    status_msg     TEXT,
    resource_attrs TEXT,
    span_attrs     TEXT,
    created_at     INTEGER DEFAULT (unixepoch())
);
CREATE TABLE IF NOT EXISTS metrics (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    name           TEXT    NOT NULL,
    description    TEXT,
    unit           TEXT,
    type           TEXT,
    timestamp_ns   INTEGER,
    value          REAL,
    resource_attrs TEXT,
    data_attrs     TEXT,
    created_at     INTEGER DEFAULT (unixepoch())
);
CREATE TABLE IF NOT EXISTS logs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp_ns   INTEGER,
    severity       TEXT,
    body           TEXT,
    trace_id       TEXT,
    span_id        TEXT,
    resource_attrs TEXT,
    log_attrs      TEXT,
    created_at     INTEGER DEFAULT (unixepoch())
);
CREATE TABLE IF NOT EXISTS anomalies (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_name TEXT    NOT NULL,
    value       REAL,
    z_score     REAL,
    mean        REAL,
    stddev      REAL,
    algorithm   TEXT,
    detected_at INTEGER DEFAULT (unixepoch())
);
CREATE TABLE IF NOT EXISTS entities (
    entity_type  TEXT NOT NULL,
    entity_id    TEXT NOT NULL,
    attrs        TEXT,
    last_seen_ns INTEGER,
    PRIMARY KEY (entity_type, entity_id)
);
CREATE TABLE IF NOT EXISTS service_topology (
    source_service  TEXT NOT NULL,
    target_service  TEXT NOT NULL,
    call_count      INTEGER DEFAULT 0,
    error_count     INTEGER DEFAULT 0,
    avg_duration_ms REAL    DEFAULT 0,
    updated_at      INTEGER DEFAULT (unixepoch()),
    PRIMARY KEY (source_service, target_service)
);
CREATE INDEX IF NOT EXISTS idx_spans_trace_id  ON spans(trace_id);
CREATE INDEX IF NOT EXISTS idx_spans_start_ns  ON spans(start_ns);
CREATE INDEX IF NOT EXISTS idx_spans_span_id   ON spans(span_id);
CREATE INDEX IF NOT EXISTS idx_spans_parent_id ON spans(parent_span_id);
CREATE INDEX IF NOT EXISTS idx_metrics_name   ON metrics(name);
CREATE INDEX IF NOT EXISTS idx_metrics_ts     ON metrics(timestamp_ns);
CREATE INDEX IF NOT EXISTS idx_logs_ts        ON logs(timestamp_ns);
`
