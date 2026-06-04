package storage

import "context"

// ---------------------------------------------------------------------------
// Row types — exported so the query API can return them as JSON.
// ---------------------------------------------------------------------------

type SpanRow struct {
	TraceID      string  `json:"trace_id"`
	SpanID       string  `json:"span_id"`
	ParentSpanID string  `json:"parent_span_id,omitempty"`
	Name         string  `json:"name"`
	Kind         int     `json:"kind"`
	StartNs      int64   `json:"start_ns"`
	EndNs        int64   `json:"end_ns"`
	DurationMs   float64 `json:"duration_ms"`
	StatusCode   int     `json:"status_code"`
	StatusMsg    string  `json:"status_msg,omitempty"`
	ResourceAttrs string `json:"resource_attrs"`
	SpanAttrs    string  `json:"span_attrs"`
}

type MetricRow struct {
	Name         string  `json:"name"`
	Description  string  `json:"description,omitempty"`
	Unit         string  `json:"unit,omitempty"`
	Type         string  `json:"type"`
	TimestampNs  int64   `json:"timestamp_ns"`
	Value        float64 `json:"value"`
	ResourceAttrs string `json:"resource_attrs"`
	DataAttrs    string  `json:"data_attrs"`
}

type LogRow struct {
	TimestampNs  int64  `json:"timestamp_ns"`
	Severity     string `json:"severity,omitempty"`
	Body         string `json:"body"`
	TraceID      string `json:"trace_id,omitempty"`
	SpanID       string `json:"span_id,omitempty"`
	ResourceAttrs string `json:"resource_attrs"`
	LogAttrs     string `json:"log_attrs"`
}

type AnomalyRow struct {
	MetricName string  `json:"metric_name"`
	Value      float64 `json:"value"`
	Score      float64 `json:"score"`
	Mean       float64 `json:"mean"`
	StdDev     float64 `json:"stddev"`
	Algorithm  string  `json:"algorithm"`
	DetectedAt int64   `json:"detected_at"`
}

// EntityRow represents a discovered entity (service, host, etc.).
type EntityRow struct {
	EntityType  string `json:"entity_type"`  // "service", "host"
	EntityID    string `json:"entity_id"`    // service name or hostname
	Attrs       string `json:"attrs"`        // JSON extra resource attributes
	LastSeenNs  int64  `json:"last_seen_ns"`
}

// TopologyEdge represents a directional call relationship between two services.
type TopologyEdge struct {
	SourceService string  `json:"source_service"`
	TargetService string  `json:"target_service"`
	CallCount     int64   `json:"call_count"`
	ErrorCount    int64   `json:"error_count"`
	AvgDurationMs float64 `json:"avg_duration_ms"`
	UpdatedAt     int64   `json:"updated_at"`
}

// ---------------------------------------------------------------------------
// Query filter types
// ---------------------------------------------------------------------------

type SpanQuery struct {
	TraceID string
	Name    string
	Service string // filter by resource_attrs["service.name"]
	From    int64  // nanoseconds
	To      int64
	Limit   int
}

type MetricQuery struct {
	Name     string
	Service  string            // filter by resource_attrs["service.name"]
	K8sAttrs map[string]string // OR-match k8s resource attrs (pod, deployment, namespace)
	From     int64
	To       int64
	Limit    int
}

type LogQuery struct {
	Severity string
	TraceID  string
	Service  string            // filter by resource_attrs["service.name"]
	K8sAttrs map[string]string // OR-match k8s resource attrs
	From     int64
	To       int64
	Limit    int
}

// ---------------------------------------------------------------------------
// Backend interface — implemented by SQLiteBackend and ClickHouseBackend.
// ---------------------------------------------------------------------------

type Backend interface {
	Init(ctx context.Context) error

	FlushSpans(ctx context.Context, batch []SpanRow) error
	FlushMetrics(ctx context.Context, metrics []MetricRow, anomalies []AnomalyRow) error
	FlushLogs(ctx context.Context, batch []LogRow) error

	QuerySpans(ctx context.Context, q SpanQuery) ([]SpanRow, error)
	QueryMetrics(ctx context.Context, q MetricQuery) ([]MetricRow, error)
	QueryLogs(ctx context.Context, q LogQuery) ([]LogRow, error)
	QueryAnomalies(ctx context.Context, limit int) ([]AnomalyRow, error)

	// Entity / topology
	UpsertEntities(ctx context.Context, entities []EntityRow) error
	RefreshTopology(ctx context.Context) error
	QueryEntities(ctx context.Context, entityType string) ([]EntityRow, error)
	QueryTopology(ctx context.Context) ([]TopologyEdge, error)

	// DeleteBefore removes rows older than cutoffUnix (Unix seconds).
	// ClickHouse implements this as a no-op since it uses table-level TTL.
	DeleteBefore(ctx context.Context, cutoffUnix int64) error

	Close() error
}
