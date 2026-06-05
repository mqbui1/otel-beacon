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
	EntityID     string  `json:"entity_id"`
	SignalType   string  `json:"signal_type"`    // "metric"|"span_error_rate"|"span_latency"|"trace_drift"|"error_signature"
	DetectorName string  `json:"detector_name"`
	MetricName   string  `json:"metric_name"`
	Value        float64 `json:"value"`
	Score        float64 `json:"score"`
	Mean         float64 `json:"mean"`
	StdDev       float64 `json:"stddev"`
	Algorithm    string  `json:"algorithm"`
	Severity     string  `json:"severity"`     // "warning"|"critical"
	Description  string  `json:"description"`
	DetectedAt   int64   `json:"detected_at"`
}

type TraceFingerprintRow struct {
	Hash            string `json:"hash"`
	RootService     string `json:"root_service"`
	EdgeList        string `json:"edge_list"`        // JSON: ["svc:op→svc:op"]
	OccurrenceCount int64  `json:"occurrence_count"`
	FirstSeenAt     int64  `json:"first_seen_at"`
	LastSeenAt      int64  `json:"last_seen_at"`
	IsBaseline      bool   `json:"is_baseline"`
}

type ErrorSignatureRow struct {
	Hash            string  `json:"hash"`
	Service         string  `json:"service"`
	ErrorType       string  `json:"error_type"`
	HTTPStatus      string  `json:"http_status"`
	Operation       string  `json:"operation"`
	OccurrenceCount int64   `json:"occurrence_count"`
	BaselineRate    float64 `json:"baseline_rate"` // occurrences per window
	FirstSeenAt     int64   `json:"first_seen_at"`
	LastSeenAt      int64   `json:"last_seen_at"`
	IsBaseline      bool    `json:"is_baseline"`
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
	TraceID       string
	Name          string
	Service       string // filter by resource_attrs["service.name"]
	StatusCode    int    // non-zero: filter status_code = value (2 = ERROR)
	From          int64  // nanoseconds
	To            int64
	Limit         int
	InternalLimit int    // bypass 1000-row cap for background workers
}

type MetricQuery struct {
	Name         string
	NamePrefixes []string          // OR-match: name LIKE 'prefix.%'
	Service      string            // filter by resource_attrs["service.name"]
	K8sAttrs     map[string]string // OR-match k8s resource attrs (pod, deployment, namespace)
	From         int64
	To           int64
	Limit        int
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
	FlushAnomalies(ctx context.Context, rows []AnomalyRow) error

	QuerySpans(ctx context.Context, q SpanQuery) ([]SpanRow, error)
	QueryMetrics(ctx context.Context, q MetricQuery) ([]MetricRow, error)
	QueryLogs(ctx context.Context, q LogQuery) ([]LogRow, error)
	QueryAnomalies(ctx context.Context, limit int) ([]AnomalyRow, error)

	// Fingerprints / error signatures
	UpsertTraceFingerprint(ctx context.Context, fp TraceFingerprintRow) error
	QueryTraceFingerprints(ctx context.Context, service string) ([]TraceFingerprintRow, error)
	UpsertErrorSignature(ctx context.Context, sig ErrorSignatureRow) error
	QueryErrorSignatures(ctx context.Context, service string) ([]ErrorSignatureRow, error)

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
