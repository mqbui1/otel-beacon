package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

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
	db.SetMaxOpenConns(8) // WAL mode: multiple concurrent readers, serialised writes
	db.SetMaxIdleConns(8)
	return &SQLiteBackend{db: db}, nil
}

func (b *SQLiteBackend) Init(ctx context.Context) error {
	if _, err := b.db.ExecContext(ctx, "PRAGMA journal_mode=WAL"); err != nil {
		return err
	}
	// 30 s busy timeout — SQLite retries at the C level before returning SQLITE_BUSY.
	// This absorbs burst contention from the many concurrent worker goroutines.
	if _, err := b.db.ExecContext(ctx, "PRAGMA busy_timeout=30000"); err != nil {
		return err
	}
	// Synchronise writes to the WAL: NORMAL is safe and much faster than FULL.
	b.db.ExecContext(ctx, "PRAGMA synchronous=NORMAL")
	// Single writer at a time — eliminate connection-pool write contention.
	b.db.SetMaxOpenConns(1)
	// Run column migrations first so the full schema (including new indexes) succeeds on existing DBs.
	b.migrateSchema(ctx)
	if _, err := b.db.ExecContext(ctx, sqliteSchema); err != nil {
		return err
	}
	// Remove stale inferred topology entries for embedded databases.
	b.db.ExecContext(ctx, `DELETE FROM service_topology WHERE target_service IN ('hsqldb','h2','derby','sqlite') OR target_service LIKE '%/%'`)
	return nil
}

// migrateSchema adds new columns/tables to existing databases without data loss.
func (b *SQLiteBackend) migrateSchema(ctx context.Context) {
	migrations := []string{
		`ALTER TABLE anomalies ADD COLUMN entity_id TEXT DEFAULT ''`,
		`ALTER TABLE anomalies ADD COLUMN signal_type TEXT DEFAULT 'metric'`,
		`ALTER TABLE anomalies ADD COLUMN detector_name TEXT DEFAULT ''`,
		`ALTER TABLE anomalies ADD COLUMN severity TEXT DEFAULT 'warning'`,
		`ALTER TABLE anomalies ADD COLUMN description TEXT DEFAULT ''`,
		`ALTER TABLE entities ADD COLUMN environment TEXT DEFAULT ''`,
		// Entity correlation: store resolved entity_id as a real indexed column.
		`ALTER TABLE spans   ADD COLUMN entity_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE metrics ADD COLUMN entity_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE logs    ADD COLUMN entity_id TEXT NOT NULL DEFAULT ''`,
		// Backfill existing rows from resource_attrs so the fast path is immediately usable.
		`UPDATE spans   SET entity_id = COALESCE(json_extract(resource_attrs, '$."service.name"'), json_extract(resource_attrs, '$."host.name"'), '') WHERE entity_id = ''`,
		`UPDATE metrics SET entity_id = COALESCE(json_extract(resource_attrs, '$."service.name"'), json_extract(resource_attrs, '$."host.name"'), '') WHERE entity_id = ''`,
		`UPDATE logs    SET entity_id = COALESCE(json_extract(resource_attrs, '$."service.name"'), json_extract(resource_attrs, '$."host.name"'), '') WHERE entity_id = ''`,
		// New eval metric columns (added in phase 1).
		`ALTER TABLE eval_results ADD COLUMN correctness           REAL NOT NULL DEFAULT 0`,
		`ALTER TABLE eval_results ADD COLUMN instruction_adherence REAL NOT NULL DEFAULT 0`,
		`ALTER TABLE eval_results ADD COLUMN reasoning_coherence   REAL NOT NULL DEFAULT 0`,
		`ALTER TABLE eval_results ADD COLUMN completeness          REAL NOT NULL DEFAULT 0`,
		// Phase 2: sessions.
		`ALTER TABLE genai_spans ADD COLUMN session_id TEXT NOT NULL DEFAULT ''`,
		// Call graph drift: track when each service→service edge was first observed.
		// NOTE: SQLite ALTER TABLE ADD COLUMN does not allow function-call defaults — use 0.
		// New rows inserted by RefreshTopology set first_seen_at explicitly via the SELECT.
		`ALTER TABLE service_topology ADD COLUMN first_seen_at INTEGER DEFAULT 0`,
	}
	for _, m := range migrations {
		b.db.ExecContext(ctx, m) // ignore "duplicate column" errors
	}
}

func (b *SQLiteBackend) Close() error { return b.db.Close() }

// ---------------------------------------------------------------------------
// Flush (batch insert via prepared statement in a transaction)
// ---------------------------------------------------------------------------

func (b *SQLiteBackend) FlushSpans(ctx context.Context, batch []SpanRow) error {
	return b.inTx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO spans
				(entity_id, trace_id, span_id, parent_span_id, name, kind,
				 start_ns, end_ns, duration_ms, status_code, status_msg,
				 resource_attrs, span_attrs)
			VALUES (?,?,?,?,?,?, ?,?,?,?,?, ?,?)`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, r := range batch {
			if _, err := stmt.ExecContext(ctx,
				r.EntityID, r.TraceID, r.SpanID, r.ParentSpanID, r.Name, r.Kind,
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
				(entity_id, name, description, unit, type, timestamp_ns, value, resource_attrs, data_attrs)
			VALUES (?,?,?,?,?,?,?,?,?)`)
		if err != nil {
			return err
		}
		defer mstmt.Close()
		for _, r := range metrics {
			if _, err := mstmt.ExecContext(ctx,
				r.EntityID, r.Name, r.Description, r.Unit, r.Type,
				r.TimestampNs, r.Value, r.ResourceAttrs, r.DataAttrs,
			); err != nil {
				return err
			}
		}
		if len(anomalies) == 0 {
			return nil
		}
		astmt, err := tx.PrepareContext(ctx, `
			INSERT INTO anomalies
				(entity_id, signal_type, detector_name, metric_name,
				 value, z_score, mean, stddev, algorithm, severity, description, detected_at)
			VALUES (?,?,?,?, ?,?,?,?,?,?,?,?)`)
		if err != nil {
			return err
		}
		defer astmt.Close()
		for _, a := range anomalies {
			if _, err := astmt.ExecContext(ctx,
				a.EntityID, a.SignalType, a.DetectorName, a.MetricName,
				a.Value, a.Score, a.Mean, a.StdDev, a.Algorithm, a.Severity, a.Description, a.DetectedAt,
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
				(entity_id, timestamp_ns, severity, body, trace_id, span_id, resource_attrs, log_attrs)
			VALUES (?,?,?,?,?,?,?,?)`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, r := range batch {
			if _, err := stmt.ExecContext(ctx,
				r.EntityID, r.TimestampNs, r.Severity, r.Body, r.TraceID, r.SpanID,
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
	// Trace-first mode: fetch N most-recent distinct traces, then return all their spans.
	// This prevents high-fan-out traces (e.g. Hibernate N+1) from crowding out other traces.
	if q.Traces > 0 {
		where, args := spanWhere(q)
		// Use the start_ns index to scan at most 20k recent rows first, then
		// group the small result — avoids full-table GROUP BY on large DBs.
		traceRows, err := b.db.QueryContext(ctx,
			`SELECT trace_id FROM (
			    SELECT trace_id, start_ns FROM spans`+where+`ORDER BY start_ns DESC LIMIT 20000
			 ) GROUP BY trace_id ORDER BY MAX(start_ns) DESC LIMIT ?`,
			append(args, q.Traces)...,
		)
		if err != nil {
			return nil, err
		}
		var traceIDs []string
		for traceRows.Next() {
			var tid string
			if err := traceRows.Scan(&tid); err != nil {
				traceRows.Close()
				return nil, err
			}
			traceIDs = append(traceIDs, tid)
		}
		traceRows.Close()
		if len(traceIDs) == 0 {
			return nil, nil
		}
		placeholders := "?" + strings.Repeat(",?", len(traceIDs)-1)
		iargs := make([]any, len(traceIDs))
		for i, id := range traceIDs {
			iargs[i] = id
		}
		rows, err := b.db.QueryContext(ctx,
			`SELECT entity_id, trace_id, span_id, parent_span_id, name, kind,
				start_ns, end_ns, duration_ms, status_code, status_msg,
				resource_attrs, span_attrs
			 FROM spans WHERE trace_id IN (`+placeholders+`) ORDER BY start_ns DESC`,
			iargs...,
		)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
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

	where, args := spanWhere(q)
	lim := limit(q.Limit)
	if q.InternalLimit > 0 {
		lim = q.InternalLimit
	}
	rows, err := b.db.QueryContext(ctx,
		`SELECT entity_id, trace_id, span_id, parent_span_id, name, kind,
			start_ns, end_ns, duration_ms, status_code, status_msg,
			resource_attrs, span_attrs
		 FROM spans`+where+` ORDER BY start_ns DESC LIMIT ?`,
		append(args, lim)...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
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

func (b *SQLiteBackend) QueryMetrics(ctx context.Context, q MetricQuery) ([]MetricRow, error) {
	where, args := metricWhere(q)
	rows, err := b.db.QueryContext(ctx,
		`SELECT entity_id, name, description, unit, type, timestamp_ns, value, resource_attrs, data_attrs
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
		if err := rows.Scan(&r.EntityID, &r.Name, &r.Description, &r.Unit, &r.Type,
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
		`SELECT entity_id, timestamp_ns, severity, body, trace_id, span_id, resource_attrs, log_attrs
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
		if err := rows.Scan(&r.EntityID, &r.TimestampNs, &r.Severity, &r.Body,
			&r.TraceID, &r.SpanID, &r.ResourceAttrs, &r.LogAttrs,
		); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (b *SQLiteBackend) QueryAnomalies(ctx context.Context, entityID string, lim int) ([]AnomalyRow, error) {
	var q string
	var args []any
	if entityID != "" {
		q = `SELECT entity_id, signal_type, detector_name, metric_name,
		            value, z_score, mean, stddev, algorithm, severity, description, detected_at
		     FROM anomalies WHERE entity_id = ? ORDER BY detected_at DESC LIMIT ?`
		args = []any{entityID, limit(lim)}
	} else {
		q = `SELECT entity_id, signal_type, detector_name, metric_name,
		            value, z_score, mean, stddev, algorithm, severity, description, detected_at
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

func (b *SQLiteBackend) ClearResolvedAnomalies(ctx context.Context, olderThanNs int64) error {
	_, err := b.db.ExecContext(ctx,
		`DELETE FROM anomalies WHERE detected_at < ?`, olderThanNs)
	return err
}

func (b *SQLiteBackend) InsertChangeEvent(ctx context.Context, e ChangeEventRow) (int64, error) {
	if e.Timestamp == 0 {
		e.Timestamp = time.Now().Unix()
	}
	if e.ChangeType == "" {
		e.ChangeType = "deploy"
	}
	res, err := b.db.ExecContext(ctx,
		`INSERT INTO change_events (entity_id, change_type, description, author, link, timestamp)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		e.EntityID, e.ChangeType, e.Description, e.Author, e.Link, e.Timestamp,
	)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (b *SQLiteBackend) QueryChangeEvents(ctx context.Context, entityID string, fromSecs, toSecs int64, lim int) ([]ChangeEventRow, error) {
	if lim <= 0 {
		lim = 50
	}
	var (
		q    string
		args []any
	)
	conds := []string{}
	if entityID != "" {
		conds = append(conds, "entity_id = ?")
		args = append(args, entityID)
	}
	if fromSecs > 0 {
		conds = append(conds, "timestamp >= ?")
		args = append(args, fromSecs)
	}
	if toSecs > 0 {
		conds = append(conds, "timestamp <= ?")
		args = append(args, toSecs)
	}
	q = `SELECT id, entity_id, change_type, description, author, link, timestamp FROM change_events`
	if len(conds) > 0 {
		q += ` WHERE ` + strings.Join(conds, " AND ")
	}
	q += ` ORDER BY timestamp DESC LIMIT ?`
	args = append(args, lim)

	rows, err := b.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChangeEventRow
	for rows.Next() {
		var r ChangeEventRow
		if err := rows.Scan(&r.ID, &r.EntityID, &r.ChangeType, &r.Description, &r.Author, &r.Link, &r.Timestamp); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (b *SQLiteBackend) UpsertIncidentGroup(ctx context.Context, g IncidentGroupRow) error {
	_, err := b.db.ExecContext(ctx,
		`INSERT INTO incident_groups
		    (group_id, root_entity_id, affected_entities, severity, status, signal_types, description, first_seen_ns, last_seen_ns, resolved_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(group_id) DO UPDATE SET
		    affected_entities = excluded.affected_entities,
		    severity          = excluded.severity,
		    status            = excluded.status,
		    signal_types      = excluded.signal_types,
		    description       = excluded.description,
		    last_seen_ns      = excluded.last_seen_ns,
		    resolved_at       = excluded.resolved_at`,
		g.GroupID, g.RootEntityID, g.AffectedEntities, g.Severity, g.Status,
		g.SignalTypes, g.Description, g.FirstSeenNs, g.LastSeenNs, g.ResolvedAt,
	)
	return err
}

func (b *SQLiteBackend) QueryIncidentGroups(ctx context.Context, status string, limit int) ([]IncidentGroupRow, error) {
	if limit <= 0 {
		limit = 50
	}
	// Fetch more rows than needed so we can deduplicate by root entity.
	fetchLimit := limit * 6
	var (
		q    string
		args []any
	)
	if status != "" {
		q = `SELECT group_id, root_entity_id, affected_entities, severity, status, signal_types, description, first_seen_ns, last_seen_ns, resolved_at
		     FROM incident_groups WHERE status = ? AND root_entity_id != ''
		     ORDER BY last_seen_ns DESC LIMIT ?`
		args = []any{status, fetchLimit}
	} else {
		q = `SELECT group_id, root_entity_id, affected_entities, severity, status, signal_types, description, first_seen_ns, last_seen_ns, resolved_at
		     FROM incident_groups WHERE root_entity_id != ''
		     ORDER BY last_seen_ns DESC LIMIT ?`
		args = []any{fetchLimit}
	}
	rows, err := b.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var raw []IncidentGroupRow
	for rows.Next() {
		var r IncidentGroupRow
		if err := rows.Scan(&r.GroupID, &r.RootEntityID, &r.AffectedEntities, &r.Severity, &r.Status,
			&r.SignalTypes, &r.Description, &r.FirstSeenNs, &r.LastSeenNs, &r.ResolvedAt); err != nil {
			return nil, err
		}
		raw = append(raw, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Deduplicate: keep only the most recent group per (root_entity_id, status).
	// Rows are ORDER BY last_seen_ns DESC so first occurrence wins.
	type dedupeKey struct{ root, status string }
	seen := make(map[dedupeKey]bool)
	out := make([]IncidentGroupRow, 0, limit)
	for _, r := range raw {
		k := dedupeKey{r.RootEntityID, r.Status}
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, r)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (b *SQLiteBackend) ResolveStaleIncidentGroups(ctx context.Context, staleSecs int64) error {
	cutoffNs := (time.Now().Unix()-staleSecs) * 1_000_000_000
	_, err := b.db.ExecContext(ctx,
		`UPDATE incident_groups SET status = 'resolved', resolved_at = ?
		 WHERE status = 'active' AND last_seen_ns < ?`,
		time.Now().UnixNano(), cutoffNs)
	return err
}

func (b *SQLiteBackend) SaveEntitySnapshot(ctx context.Context, s EntitySnapshotRow) error {
	_, err := b.db.ExecContext(ctx,
		`INSERT INTO entity_snapshots (entity_id, snapshot_at, health_json, group_id) VALUES (?, ?, ?, ?)`,
		s.EntityID, s.SnapshotAt, s.HealthJSON, s.GroupID,
	)
	return err
}

func (b *SQLiteBackend) QueryEntitySnapshot(ctx context.Context, entityID string, nearNs int64) (*EntitySnapshotRow, error) {
	windowNs := int64(15 * 60 * 1_000_000_000)
	row := b.db.QueryRowContext(ctx,
		`SELECT id, entity_id, snapshot_at, health_json, group_id FROM entity_snapshots
		 WHERE entity_id = ? AND snapshot_at BETWEEN ? AND ?
		 ORDER BY ABS(snapshot_at - ?) LIMIT 1`,
		entityID, nearNs-windowNs, nearNs+windowNs, nearNs)
	var s EntitySnapshotRow
	if err := row.Scan(&s.ID, &s.EntityID, &s.SnapshotAt, &s.HealthJSON, &s.GroupID); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &s, nil
}

func (b *SQLiteBackend) ResetSimulationData(ctx context.Context) error {
	// Skip spans/metrics/logs — those large tables age out via the retention worker.
	// Deleting them on a 30GB DB holds the single SQLite connection for minutes,
	// blocking all HTTP reads. Only clear the small derived-data tables.
	for _, q := range []string{
		`DELETE FROM entities`,
		`DELETE FROM anomalies`,
		`DELETE FROM error_signatures`,
		`DELETE FROM trace_fingerprints`,
		`DELETE FROM service_topology`,
		`DELETE FROM incident_groups`,
		`DELETE FROM entity_snapshots`,
	} {
		if _, err := b.db.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func (b *SQLiteBackend) DeleteMissingServiceAnomaly(ctx context.Context, entityID string) error {
	_, err := b.db.ExecContext(ctx,
		`DELETE FROM anomalies WHERE entity_id = ? AND signal_type = 'missing_service'`,
		entityID)
	return err
}

func (b *SQLiteBackend) FlushAnomalies(ctx context.Context, rows []AnomalyRow) error {
	if len(rows) == 0 {
		return nil
	}
	return b.inTx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO anomalies
				(entity_id, signal_type, detector_name, metric_name,
				 value, z_score, mean, stddev, algorithm, severity, description, detected_at)
			VALUES (?,?,?,?, ?,?,?,?,?,?,?,?)`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, a := range rows {
			if _, err := stmt.ExecContext(ctx,
				a.EntityID, a.SignalType, a.DetectorName, a.MetricName,
				a.Value, a.Score, a.Mean, a.StdDev, a.Algorithm, a.Severity, a.Description, a.DetectedAt,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *SQLiteBackend) UpsertTraceFingerprint(ctx context.Context, fp TraceFingerprintRow) error {
	isBaseline := 0
	if fp.IsBaseline {
		isBaseline = 1
	}
	_, err := b.db.ExecContext(ctx, `
		INSERT INTO trace_fingerprints
			(hash, root_service, edge_list, occurrence_count, first_seen_at, last_seen_at, is_baseline)
		VALUES (?,?,?,?,?,?,?)
		ON CONFLICT(hash) DO UPDATE SET
			occurrence_count = excluded.occurrence_count,
			last_seen_at     = excluded.last_seen_at,
			is_baseline      = MAX(is_baseline, excluded.is_baseline)`,
		fp.Hash, fp.RootService, fp.EdgeList,
		fp.OccurrenceCount, fp.FirstSeenAt, fp.LastSeenAt, isBaseline)
	return err
}

func (b *SQLiteBackend) QueryTraceFingerprints(ctx context.Context, service string) ([]TraceFingerprintRow, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if service != "" {
		rows, err = b.db.QueryContext(ctx,
			`SELECT hash, root_service, edge_list, occurrence_count, first_seen_at, last_seen_at, is_baseline
			 FROM trace_fingerprints WHERE root_service = ?`, service)
	} else {
		rows, err = b.db.QueryContext(ctx,
			`SELECT hash, root_service, edge_list, occurrence_count, first_seen_at, last_seen_at, is_baseline
			 FROM trace_fingerprints`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TraceFingerprintRow
	for rows.Next() {
		var fp TraceFingerprintRow
		var isBaseline int
		if err := rows.Scan(&fp.Hash, &fp.RootService, &fp.EdgeList,
			&fp.OccurrenceCount, &fp.FirstSeenAt, &fp.LastSeenAt, &isBaseline); err != nil {
			return nil, err
		}
		fp.IsBaseline = isBaseline == 1
		out = append(out, fp)
	}
	return out, rows.Err()
}

func (b *SQLiteBackend) UpsertErrorSignature(ctx context.Context, sig ErrorSignatureRow) error {
	isBaseline := 0
	if sig.IsBaseline {
		isBaseline = 1
	}
	_, err := b.db.ExecContext(ctx, `
		INSERT INTO error_signatures
			(hash, service, error_type, http_status, operation,
			 occurrence_count, baseline_rate, first_seen_at, last_seen_at, is_baseline)
		VALUES (?,?,?,?,?, ?,?,?,?,?)
		ON CONFLICT(hash) DO UPDATE SET
			occurrence_count = excluded.occurrence_count,
			baseline_rate    = excluded.baseline_rate,
			last_seen_at     = excluded.last_seen_at,
			is_baseline      = MAX(is_baseline, excluded.is_baseline)`,
		sig.Hash, sig.Service, sig.ErrorType, sig.HTTPStatus, sig.Operation,
		sig.OccurrenceCount, sig.BaselineRate, sig.FirstSeenAt, sig.LastSeenAt, isBaseline)
	return err
}

func (b *SQLiteBackend) QueryErrorSignatures(ctx context.Context, service string) ([]ErrorSignatureRow, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if service != "" {
		rows, err = b.db.QueryContext(ctx,
			`SELECT hash, service, error_type, http_status, operation,
			        occurrence_count, baseline_rate, first_seen_at, last_seen_at, is_baseline
			 FROM error_signatures WHERE service = ?`, service)
	} else {
		rows, err = b.db.QueryContext(ctx,
			`SELECT hash, service, error_type, http_status, operation,
			        occurrence_count, baseline_rate, first_seen_at, last_seen_at, is_baseline
			 FROM error_signatures`)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ErrorSignatureRow
	for rows.Next() {
		var sig ErrorSignatureRow
		var isBaseline int
		if err := rows.Scan(&sig.Hash, &sig.Service, &sig.ErrorType, &sig.HTTPStatus, &sig.Operation,
			&sig.OccurrenceCount, &sig.BaselineRate, &sig.FirstSeenAt, &sig.LastSeenAt, &isBaseline); err != nil {
			return nil, err
		}
		sig.IsBaseline = isBaseline == 1
		out = append(out, sig)
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
		// Fast path: entity_id column (indexed). Legacy fallback for pre-migration rows.
		clauses = append(clauses, `(entity_id = ? OR (entity_id = '' AND json_extract(resource_attrs, '$."service.name"') = ?))`)
		args = append(args, q.Service, q.Service)
	}
	if q.StatusCode != 0 {
		clauses = append(clauses, "status_code = ?")
		args = append(args, q.StatusCode)
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
	if len(q.NamePrefixes) > 0 {
		parts := make([]string, len(q.NamePrefixes))
		for i, p := range q.NamePrefixes {
			parts[i] = "name LIKE ?"
			args = append(args, p+".%")
		}
		clauses = append(clauses, "("+strings.Join(parts, " OR ")+")")
	}
	if q.Service != "" || len(q.K8sAttrs) > 0 {
		sub, subArgs := serviceOrK8sClauses(q.Service, q.K8sAttrs)
		clauses = append(clauses, sub)
		args = append(args, subArgs...)
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
	if q.Service != "" || len(q.K8sAttrs) > 0 {
		sub, subArgs := serviceOrK8sClauses(q.Service, q.K8sAttrs)
		clauses = append(clauses, sub)
		args = append(args, subArgs...)
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

// serviceOrK8sClauses builds an OR clause for entity-based signal correlation.
// Primary path: entity_id column (indexed, populated at ingest).
// Fallback: json_extract on resource_attrs for rows written before the migration
// and for k8s-attributed signals that don't carry service.name.
// Only a fixed whitelist of k8s keys is accepted to prevent injection.
func serviceOrK8sClauses(service string, k8sAttrs map[string]string) (string, []any) {
	var parts []string
	var args []any
	if service != "" {
		// Fast path: entity_id column (indexed; covers both service and host entities).
		parts = append(parts, "entity_id = ?")
		args = append(args, service)
		// Legacy fallback: rows written before entity_id was stored have entity_id = ''.
		parts = append(parts, `(entity_id = '' AND json_extract(resource_attrs, '$."service.name"') = ?)`)
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
		// k8s attrs are only needed for infra signals without service.name (entity_id='').
		parts = append(parts, `(entity_id = '' AND json_extract(resource_attrs, '$."`+k+`"') = ?)`)
		args = append(args, v)
	}
	if len(parts) == 0 {
		return "1=1", nil
	}
	return "(" + strings.Join(parts, " OR ") + ")", args
}

func whereClause(clauses []string) string {
	if len(clauses) == 0 {
		return ""
	}
	return " WHERE " + strings.Join(clauses, " AND ")
}

func limit(n int) int {
	if n <= 0 {
		return 100
	}
	if n > 5000 {
		return 5000
	}
	return n
}

// ---------------------------------------------------------------------------
// Entity / topology
// ---------------------------------------------------------------------------

func (b *SQLiteBackend) UpsertEntities(ctx context.Context, entities []EntityRow) error {
	return b.inTx(ctx, func(tx *sql.Tx) error {
		stmt, err := tx.PrepareContext(ctx, `
			INSERT INTO entities (entity_type, entity_id, environment, attrs, last_seen_ns)
			VALUES (?, ?, ?, ?, ?)
			ON CONFLICT(entity_type, entity_id) DO UPDATE SET
				environment  = CASE WHEN excluded.environment != '' THEN excluded.environment ELSE entities.environment END,
				attrs        = excluded.attrs,
				last_seen_ns = MAX(entities.last_seen_ns, excluded.last_seen_ns)`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, e := range entities {
			if _, err := stmt.ExecContext(ctx, e.EntityType, e.EntityID, e.Environment, e.Attrs, e.LastSeenNs); err != nil {
				return err
			}
		}
		return nil
	})
}

func (b *SQLiteBackend) RefreshTopology(ctx context.Context) error {
	// Real service-to-service edges derived from parent→child span relationships.
	// ON CONFLICT: update stats but never overwrite first_seen_at so drift detection works.
	if _, err := b.db.ExecContext(ctx, `
		INSERT INTO service_topology
			(source_service, target_service, call_count, error_count, avg_duration_ms, updated_at, first_seen_at)
		SELECT
			json_extract(parent.resource_attrs, '$."service.name"') AS source_service,
			json_extract(child.resource_attrs,  '$."service.name"') AS target_service,
			COUNT(*)                                                 AS call_count,
			SUM(CASE WHEN child.status_code = 2 THEN 1 ELSE 0 END)  AS error_count,
			AVG(child.duration_ms)                                   AS avg_duration_ms,
			unixepoch()                                              AS updated_at,
			unixepoch()                                              AS first_seen_at
		FROM spans child
		JOIN spans parent ON child.parent_span_id = parent.span_id
		WHERE child.start_ns > (unixepoch() - 3600) * 1000000000
		  AND json_extract(parent.resource_attrs, '$."service.name"') IS NOT NULL
		  AND json_extract(child.resource_attrs,  '$."service.name"') IS NOT NULL
		  AND json_extract(parent.resource_attrs, '$."service.name"')
		   != json_extract(child.resource_attrs,  '$."service.name"')
		GROUP BY source_service, target_service
		ON CONFLICT(source_service, target_service) DO UPDATE SET
			call_count      = excluded.call_count,
			error_count     = excluded.error_count,
			avg_duration_ms = excluded.avg_duration_ms,
			updated_at      = excluded.updated_at`); err != nil {
		return err
	}

	// Inferred external service edges: CLIENT spans with db.system or peer.service that
	// have no server-side counterpart (e.g. MySQL, Redis, external APIs).
	// Only inserted when the target is not already a known instrumented service.
	_, err := b.db.ExecContext(ctx, `
		INSERT INTO service_topology
			(source_service, target_service, call_count, error_count, avg_duration_ms, updated_at, first_seen_at)
		SELECT src, tgt, COUNT(*), SUM(is_err), AVG(dur), unixepoch(), unixepoch()
		FROM (
			SELECT
				json_extract(s.resource_attrs, '$."service.name"') AS src,
				COALESCE(
					json_extract(s.span_attrs, '$."peer.service"'),
					json_extract(s.span_attrs, '$."db.system"')
				) AS tgt,
				CASE WHEN s.status_code = 2 THEN 1 ELSE 0 END AS is_err,
				s.duration_ms AS dur
			FROM spans s
			WHERE s.start_ns > (unixepoch() - 3600) * 1000000000
			  AND json_extract(s.resource_attrs, '$."service.name"') IS NOT NULL
			  AND (
				  (
					  json_extract(s.span_attrs, '$."db.system"') IS NOT NULL
					  -- Skip embedded/in-process databases — they are not separate services
					  AND json_extract(s.span_attrs, '$."db.system"') NOT IN ('hsqldb','h2','derby','sqlite','other_sql')
				  )
				  OR json_extract(s.span_attrs, '$."peer.service"') IS NOT NULL
			  )
		)
		WHERE tgt IS NOT NULL AND tgt != src
		  AND tgt NOT IN (
			  SELECT DISTINCT json_extract(resource_attrs, '$."service.name"')
			  FROM spans
			  WHERE start_ns > (unixepoch() - 3600) * 1000000000
				AND json_extract(resource_attrs, '$."service.name"') IS NOT NULL
		  )
		GROUP BY src, tgt
		ON CONFLICT(source_service, target_service) DO UPDATE SET
			call_count      = excluded.call_count,
			error_count     = excluded.error_count,
			avg_duration_ms = excluded.avg_duration_ms,
			updated_at      = excluded.updated_at`)

	// Remove stale edges that had no spans in the current refresh window.
	// These are rows whose updated_at is older than ~90 seconds before now,
	// meaning RefreshTopology did not touch them in this pass.
	_, err = b.db.ExecContext(ctx,
		`DELETE FROM service_topology WHERE updated_at < unixepoch() - 90`)
	return err
}

func (b *SQLiteBackend) QueryEntities(ctx context.Context, entityType, env string) ([]EntityRow, error) {
	where := "WHERE 1=1"
	var args []any
	if entityType != "" {
		where += " AND entity_type = ?"
		args = append(args, entityType)
	}
	if env != "" {
		where += " AND environment = ?"
		args = append(args, env)
	}
	rows, err := b.db.QueryContext(ctx,
		`SELECT entity_type, entity_id, COALESCE(environment,''), attrs, last_seen_ns FROM entities
		 `+where+` ORDER BY last_seen_ns DESC`,
		args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EntityRow
	for rows.Next() {
		var r EntityRow
		if err := rows.Scan(&r.EntityType, &r.EntityID, &r.Environment, &r.Attrs, &r.LastSeenNs); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (b *SQLiteBackend) QueryEnvironments(ctx context.Context) ([]string, error) {
	rows, err := b.db.QueryContext(ctx,
		`SELECT DISTINCT environment FROM entities
		 WHERE environment != '' ORDER BY environment`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var env string
		if err := rows.Scan(&env); err != nil {
			return nil, err
		}
		out = append(out, env)
	}
	return out, rows.Err()
}

func (b *SQLiteBackend) QueryTopology(ctx context.Context) ([]TopologyEdge, error) {
	rows, err := b.db.QueryContext(ctx,
		`SELECT source_service, target_service, call_count, error_count, avg_duration_ms, updated_at, COALESCE(first_seen_at,0)
		 FROM service_topology ORDER BY call_count DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TopologyEdge
	for rows.Next() {
		var e TopologyEdge
		if err := rows.Scan(&e.SourceService, &e.TargetService, &e.CallCount, &e.ErrorCount, &e.AvgDurationMs, &e.UpdatedAt, &e.FirstSeenAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (b *SQLiteBackend) QueryNewTopologyEdges(ctx context.Context, sinceSeconds int64) ([]TopologyEdge, error) {
	cutoff := time.Now().Unix() - sinceSeconds
	rows, err := b.db.QueryContext(ctx,
		`SELECT source_service, target_service, call_count, error_count, avg_duration_ms, updated_at, first_seen_at
		 FROM service_topology WHERE first_seen_at >= ? ORDER BY first_seen_at DESC`,
		cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TopologyEdge
	for rows.Next() {
		var e TopologyEdge
		if err := rows.Scan(&e.SourceService, &e.TargetService, &e.CallCount, &e.ErrorCount, &e.AvgDurationMs, &e.UpdatedAt, &e.FirstSeenAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (b *SQLiteBackend) QueryRecentAnomaliesByEntity(ctx context.Context, windowSeconds int64) (map[string][]AnomalyRow, error) {
	cutoff := time.Now().Unix() - windowSeconds
	rows, err := b.db.QueryContext(ctx,
		`SELECT entity_id, signal_type, detector_name, metric_name, value, z_score, mean, stddev, algorithm, severity, description, detected_at
		 FROM anomalies WHERE detected_at >= ? ORDER BY detected_at DESC`,
		cutoff*1_000_000_000) // detected_at is nanoseconds
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string][]AnomalyRow)
	for rows.Next() {
		var a AnomalyRow
		if err := rows.Scan(&a.EntityID, &a.SignalType, &a.DetectorName, &a.MetricName,
			&a.Value, &a.Score, &a.Mean, &a.StdDev, &a.Algorithm, &a.Severity, &a.Description, &a.DetectedAt); err != nil {
			return nil, err
		}
		out[a.EntityID] = append(out[a.EntityID], a)
	}
	return out, rows.Err()
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

const sqliteSchema = `
CREATE TABLE IF NOT EXISTS spans (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id      TEXT    NOT NULL DEFAULT '',
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
    entity_id      TEXT    NOT NULL DEFAULT '',
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
    entity_id      TEXT    NOT NULL DEFAULT '',
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
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id     TEXT    DEFAULT '',
    signal_type   TEXT    DEFAULT 'metric',
    detector_name TEXT    DEFAULT '',
    metric_name   TEXT    NOT NULL,
    value         REAL,
    z_score       REAL,
    mean          REAL,
    stddev        REAL,
    algorithm     TEXT,
    severity      TEXT    DEFAULT 'warning',
    description   TEXT    DEFAULT '',
    detected_at   INTEGER DEFAULT (unixepoch())
);
CREATE TABLE IF NOT EXISTS trace_fingerprints (
    hash             TEXT    PRIMARY KEY,
    root_service     TEXT    NOT NULL,
    edge_list        TEXT,
    occurrence_count INTEGER DEFAULT 0,
    first_seen_at    INTEGER DEFAULT (unixepoch()),
    last_seen_at     INTEGER DEFAULT (unixepoch()),
    is_baseline      INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS error_signatures (
    hash             TEXT    PRIMARY KEY,
    service          TEXT    NOT NULL,
    error_type       TEXT,
    http_status      TEXT,
    operation        TEXT,
    occurrence_count INTEGER DEFAULT 0,
    baseline_rate    REAL    DEFAULT 0,
    first_seen_at    INTEGER DEFAULT (unixepoch()),
    last_seen_at     INTEGER DEFAULT (unixepoch()),
    is_baseline      INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS entities (
    entity_type  TEXT NOT NULL,
    entity_id    TEXT NOT NULL,
    environment  TEXT NOT NULL DEFAULT '',
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
    first_seen_at   INTEGER DEFAULT 0,
    PRIMARY KEY (source_service, target_service)
);
CREATE TABLE IF NOT EXISTS change_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id    TEXT    NOT NULL DEFAULT '',
    change_type  TEXT    NOT NULL DEFAULT 'deploy',
    description  TEXT    NOT NULL DEFAULT '',
    author       TEXT    NOT NULL DEFAULT '',
    link         TEXT    NOT NULL DEFAULT '',
    timestamp    INTEGER NOT NULL DEFAULT (unixepoch()),
    created_at   INTEGER NOT NULL DEFAULT (unixepoch())
);
CREATE INDEX IF NOT EXISTS idx_change_events_entity ON change_events(entity_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_change_events_ts     ON change_events(timestamp DESC);
CREATE TABLE IF NOT EXISTS incident_groups (
    group_id          TEXT    PRIMARY KEY,
    root_entity_id    TEXT    NOT NULL DEFAULT '',
    affected_entities TEXT    NOT NULL DEFAULT '[]',
    severity          TEXT    NOT NULL DEFAULT 'warning',
    status            TEXT    NOT NULL DEFAULT 'active',
    signal_types      TEXT    NOT NULL DEFAULT '[]',
    description       TEXT    NOT NULL DEFAULT '',
    first_seen_ns     INTEGER NOT NULL DEFAULT 0,
    last_seen_ns      INTEGER NOT NULL DEFAULT 0,
    resolved_at       INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_incident_groups_status ON incident_groups(status, last_seen_ns DESC);
CREATE TABLE IF NOT EXISTS entity_snapshots (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_id   TEXT    NOT NULL,
    snapshot_at INTEGER NOT NULL,
    health_json TEXT    NOT NULL DEFAULT '{}',
    group_id    TEXT    NOT NULL DEFAULT '',
    created_at  INTEGER DEFAULT (unixepoch())
);
CREATE INDEX IF NOT EXISTS idx_entity_snapshots_entity ON entity_snapshots(entity_id, snapshot_at DESC);
CREATE TABLE IF NOT EXISTS genai_spans (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    trace_id       TEXT    NOT NULL,
    span_id        TEXT    NOT NULL,
    parent_span_id TEXT,
    entity_id      TEXT    NOT NULL DEFAULT '',
    system         TEXT    NOT NULL DEFAULT '',
    operation      TEXT    NOT NULL DEFAULT '',
    model          TEXT    NOT NULL DEFAULT '',
    agent_name     TEXT    NOT NULL DEFAULT '',
    tool_name      TEXT    NOT NULL DEFAULT '',
    input_tokens   INTEGER NOT NULL DEFAULT 0,
    output_tokens  INTEGER NOT NULL DEFAULT 0,
    total_cost_usd REAL    NOT NULL DEFAULT 0,
    start_ns       INTEGER NOT NULL DEFAULT 0,
    duration_ms    REAL    NOT NULL DEFAULT 0,
    status_code    INTEGER NOT NULL DEFAULT 0,
    prompt         TEXT    NOT NULL DEFAULT '',
    completion     TEXT    NOT NULL DEFAULT '',
    session_id     TEXT    NOT NULL DEFAULT '',
    span_attrs     TEXT    NOT NULL DEFAULT '{}',
    resource_attrs TEXT    NOT NULL DEFAULT '{}',
    created_at     INTEGER DEFAULT (unixepoch())
);
CREATE INDEX IF NOT EXISTS idx_genai_spans_trace  ON genai_spans(trace_id);
CREATE INDEX IF NOT EXISTS idx_genai_spans_entity ON genai_spans(entity_id);
CREATE INDEX IF NOT EXISTS idx_genai_spans_model  ON genai_spans(model);
CREATE INDEX IF NOT EXISTS idx_genai_spans_agent  ON genai_spans(agent_name);
CREATE INDEX IF NOT EXISTS idx_genai_spans_start   ON genai_spans(start_ns);
CREATE INDEX IF NOT EXISTS idx_genai_spans_session ON genai_spans(session_id);
CREATE TABLE IF NOT EXISTS sessions (
    session_id         TEXT    PRIMARY KEY,
    entity_id          TEXT    NOT NULL DEFAULT '',
    trace_count        INTEGER NOT NULL DEFAULT 0,
    span_count         INTEGER NOT NULL DEFAULT 0,
    total_cost_usd     REAL    NOT NULL DEFAULT 0,
    total_tokens       INTEGER NOT NULL DEFAULT 0,
    start_ns           INTEGER NOT NULL DEFAULT 0,
    last_seen_ns       INTEGER NOT NULL DEFAULT 0,
    duration_ms        REAL    NOT NULL DEFAULT 0,
    action_completion  REAL    NOT NULL DEFAULT 0,
    agent_efficiency   REAL    NOT NULL DEFAULT 0,
    conv_quality       REAL    NOT NULL DEFAULT 0,
    user_intent_change REAL    NOT NULL DEFAULT 0,
    eval_reasoning     TEXT    NOT NULL DEFAULT '',
    eval_at            INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_sessions_entity   ON sessions(entity_id);
CREATE INDEX IF NOT EXISTS idx_sessions_last_seen ON sessions(last_seen_ns);
CREATE TABLE IF NOT EXISTS eval_results (
    span_id               TEXT    PRIMARY KEY,
    trace_id              TEXT    NOT NULL,
    hallucination         REAL    NOT NULL DEFAULT 0,
    coherence             REAL    NOT NULL DEFAULT 0,
    relevance             REAL    NOT NULL DEFAULT 0,
    toxicity              REAL    NOT NULL DEFAULT 0,
    correctness           REAL    NOT NULL DEFAULT 0,
    instruction_adherence REAL    NOT NULL DEFAULT 0,
    reasoning_coherence   REAL    NOT NULL DEFAULT 0,
    completeness          REAL    NOT NULL DEFAULT 0,
    overall_score         REAL    NOT NULL DEFAULT 0,
    reasoning             TEXT    NOT NULL DEFAULT '',
    evaluated_at          INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_eval_results_trace ON eval_results(trace_id);
CREATE TABLE IF NOT EXISTS guardrail_events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    span_id    TEXT    NOT NULL,
    trace_id   TEXT    NOT NULL,
    check_type TEXT    NOT NULL,
    triggered  INTEGER NOT NULL DEFAULT 0,
    severity   TEXT    NOT NULL DEFAULT '',
    detail     TEXT    NOT NULL DEFAULT '',
    checked_at INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_guardrail_trace ON guardrail_events(trace_id);
CREATE INDEX IF NOT EXISTS idx_guardrail_triggered ON guardrail_events(triggered);
CREATE INDEX IF NOT EXISTS idx_spans_entity_id ON spans(entity_id);
CREATE INDEX IF NOT EXISTS idx_spans_trace_id  ON spans(trace_id);
CREATE INDEX IF NOT EXISTS idx_spans_start_ns  ON spans(start_ns);
CREATE INDEX IF NOT EXISTS idx_spans_span_id   ON spans(span_id);
CREATE INDEX IF NOT EXISTS idx_spans_parent_id ON spans(parent_span_id);
CREATE INDEX IF NOT EXISTS idx_metrics_name      ON metrics(name);
CREATE INDEX IF NOT EXISTS idx_metrics_ts        ON metrics(timestamp_ns);
CREATE INDEX IF NOT EXISTS idx_metrics_entity_id ON metrics(entity_id);
CREATE INDEX IF NOT EXISTS idx_logs_ts           ON logs(timestamp_ns);
CREATE INDEX IF NOT EXISTS idx_logs_entity_id    ON logs(entity_id);
CREATE TABLE IF NOT EXISTS datasets (
    dataset_id TEXT    PRIMARY KEY,
    name       TEXT    NOT NULL DEFAULT '',
    entity_id  TEXT    NOT NULL DEFAULT '',
    row_count  INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER DEFAULT (unixepoch())
);
CREATE TABLE IF NOT EXISTS dataset_rows (
    row_id     TEXT PRIMARY KEY,
    dataset_id TEXT NOT NULL,
    prompt     TEXT NOT NULL DEFAULT '',
    completion TEXT NOT NULL DEFAULT '',
    context    TEXT NOT NULL DEFAULT '',
    expected   TEXT NOT NULL DEFAULT '',
    created_at INTEGER DEFAULT (unixepoch())
);
CREATE INDEX IF NOT EXISTS idx_dataset_rows_dataset ON dataset_rows(dataset_id);
CREATE TABLE IF NOT EXISTS experiments (
    experiment_id             TEXT    PRIMARY KEY,
    name                      TEXT    NOT NULL DEFAULT '',
    dataset_id                TEXT    NOT NULL DEFAULT '',
    entity_id                 TEXT    NOT NULL DEFAULT '',
    status                    TEXT    NOT NULL DEFAULT 'pending',
    row_count                 INTEGER NOT NULL DEFAULT 0,
    scored_count              INTEGER NOT NULL DEFAULT 0,
    avg_overall               REAL    NOT NULL DEFAULT 0,
    avg_hallucination         REAL    NOT NULL DEFAULT 0,
    avg_coherence             REAL    NOT NULL DEFAULT 0,
    avg_relevance             REAL    NOT NULL DEFAULT 0,
    avg_toxicity              REAL    NOT NULL DEFAULT 0,
    avg_correctness           REAL    NOT NULL DEFAULT 0,
    avg_instruction_adherence REAL    NOT NULL DEFAULT 0,
    avg_reasoning_coherence   REAL    NOT NULL DEFAULT 0,
    avg_completeness          REAL    NOT NULL DEFAULT 0,
    created_at                INTEGER DEFAULT (unixepoch()),
    completed_at              INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS experiment_results (
    result_id             TEXT PRIMARY KEY,
    experiment_id         TEXT NOT NULL,
    row_id                TEXT NOT NULL,
    overall_score         REAL NOT NULL DEFAULT 0,
    hallucination         REAL NOT NULL DEFAULT 0,
    coherence             REAL NOT NULL DEFAULT 0,
    relevance             REAL NOT NULL DEFAULT 0,
    toxicity              REAL NOT NULL DEFAULT 0,
    correctness           REAL NOT NULL DEFAULT 0,
    instruction_adherence REAL NOT NULL DEFAULT 0,
    reasoning_coherence   REAL NOT NULL DEFAULT 0,
    completeness          REAL NOT NULL DEFAULT 0,
    eval_reasoning        TEXT NOT NULL DEFAULT '',
    created_at            INTEGER DEFAULT (unixepoch())
);
CREATE INDEX IF NOT EXISTS idx_exp_results_exp ON experiment_results(experiment_id);
CREATE TABLE IF NOT EXISTS custom_metrics (
    metric_id   TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    description TEXT DEFAULT '',
    prompt      TEXT NOT NULL,
    output_type TEXT DEFAULT 'boolean',
    apply_to    TEXT DEFAULT 'span',
    action      TEXT DEFAULT 'alert',
    created_at  INTEGER DEFAULT (unixepoch())
);
CREATE TABLE IF NOT EXISTS custom_metric_results (
    result_id    TEXT PRIMARY KEY,
    metric_id    TEXT,
    metric_name  TEXT,
    span_id      TEXT,
    trace_id     TEXT,
    value_bool   INTEGER,
    value_score  REAL DEFAULT 0,
    reasoning    TEXT DEFAULT '',
    evaluated_at INTEGER DEFAULT (unixepoch())
);
CREATE INDEX IF NOT EXISTS idx_cmr_metric ON custom_metric_results(metric_id);
CREATE INDEX IF NOT EXISTS idx_cmr_span   ON custom_metric_results(span_id);
CREATE INDEX IF NOT EXISTS idx_cmr_trace  ON custom_metric_results(trace_id);
CREATE TABLE IF NOT EXISTS eval_feedback (
    feedback_id      TEXT PRIMARY KEY,
    span_id          TEXT,
    metric_name      TEXT,
    original_value   REAL DEFAULT 0,
    corrected_value  REAL,
    rationale        TEXT DEFAULT '',
    created_at       INTEGER DEFAULT (unixepoch())
);
CREATE INDEX IF NOT EXISTS idx_feedback_span ON eval_feedback(span_id);
`
