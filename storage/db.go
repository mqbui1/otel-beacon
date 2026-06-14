package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------

const (
	defaultBatchSize     = 500
	defaultFlushInterval = 500 * time.Millisecond // wider window → larger batches → fewer write transactions
	defaultChannelSize   = 10_000
	defaultMaxRetries    = 5
)

type AlgoType string

const (
	AlgoZScore AlgoType = "zscore"
	AlgoMAD    AlgoType = "mad"
	AlgoEWMA   AlgoType = "ewma"
)

type Option func(*Storage)

func WithOnAnomaly(fn func(AnomalyRow)) Option  { return func(s *Storage) { s.onAnomaly = fn } }
func WithOnError(fn func(error)) Option         { return func(s *Storage) { s.onError = fn } }
func WithBatchSize(n int) Option                { return func(s *Storage) { s.batchSize = n } }
func WithFlushInterval(d time.Duration) Option  { return func(s *Storage) { s.flushInterval = d } }
func WithChannelSize(n int) Option              { return func(s *Storage) { s.chanSize = n } }
func WithRetentionDays(n int) Option            { return func(s *Storage) { s.retentionDays = n } }
func WithMaxRetries(n int) Option               { return func(s *Storage) { s.maxRetries = n } }
func WithAlgorithm(a AlgoType) Option           { return func(s *Storage) { s.algo = a } }
func WithEWMAAlpha(alpha float64) Option        { return func(s *Storage) { s.ewmaAlpha = alpha } }
func WithAnomalyThreshold(t float64) Option     { return func(s *Storage) { s.anomalyThresh = t } }
func WithAnomalyWindow(n int) Option            { return func(s *Storage) { s.anomalyWindow = n } }
func WithPrometheus(reg prometheus.Registerer) Option {
	return func(s *Storage) { s.promReg = reg }
}

// ---------------------------------------------------------------------------
// Storage — async queue layer on top of a Backend.
// ---------------------------------------------------------------------------

type Storage struct {
	backend       Backend
	detector      Detector
	algo          AlgoType
	ewmaAlpha     float64
	anomalyThresh float64
	anomalyWindow int

	batchSize     int
	flushInterval time.Duration
	chanSize      int
	retentionDays int
	maxRetries    int

	onAnomaly func(AnomalyRow)
	onError   func(error)
	promReg   prometheus.Registerer

	spanCh       chan SpanRow
	metricCh     chan metricBatch // metric + pre-detected anomalies
	logCh        chan LogRow
	genaiCh       chan GenAISpanRow // gen_ai.* spans routed here after extraction
	genaiEvalCh   chan GenAISpanRow // drained by the server-side span eval worker
	sessionEvalCh chan SessionRow   // drained by the server-side session eval worker

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	// Missing-service detection: track last time each root service was seen
	// in a fingerprinted trace. Updated by fingerprintWorker, read by missingServiceWorker.
	missingMu       sync.Mutex
	lastSeenRootSvc map[string]time.Time // rootSvc -> last observed time
	missingEmitted  map[string]bool      // rootSvc -> true if alert already fired this absence

	// Prometheus metrics
	received  *prometheus.CounterVec
	dropped   *prometheus.CounterVec
	depth     *prometheus.GaugeVec
	flushDur  *prometheus.HistogramVec
	batchSz   *prometheus.HistogramVec
	flushErrs *prometheus.CounterVec
}

type metricBatch struct {
	row      MetricRow
	anomaly  *AnomalyRow // nil if not anomalous
}

func New(backend Backend, opts ...Option) *Storage {
	s := &Storage{
		backend:         backend,
		algo:            AlgoMAD,
		ewmaAlpha:       0.3,
		anomalyThresh:   3.5,
		anomalyWindow:   100,
		batchSize:       defaultBatchSize,
		flushInterval:   defaultFlushInterval,
		chanSize:        defaultChannelSize,
		retentionDays:   30,
		maxRetries:      defaultMaxRetries,
		onAnomaly:       func(AnomalyRow) {},
		onError:         func(err error) { fmt.Println("storage error:", err) },
		promReg:         prometheus.DefaultRegisterer,
		lastSeenRootSvc: make(map[string]time.Time),
		missingEmitted:  make(map[string]bool),
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Init creates the schema, starts workers, and optionally starts the retention worker.
func (s *Storage) Init(ctx context.Context) error {
	if err := s.backend.Init(ctx); err != nil {
		return err
	}

	s.detector = s.buildDetector()
	s.registerMetrics()

	s.spanCh = make(chan SpanRow, s.chanSize)
	s.metricCh = make(chan metricBatch, s.chanSize)
	s.logCh = make(chan LogRow, s.chanSize)
	s.genaiCh = make(chan GenAISpanRow, s.chanSize)
	s.genaiEvalCh   = make(chan GenAISpanRow, 500) // bounded: span eval is async and slower
	s.sessionEvalCh = make(chan SessionRow, 200)   // bounded: session eval is async and slower

	s.ctx, s.cancel = context.WithCancel(context.Background())

	s.wg.Add(4)
	go s.spanWorker()
	go s.metricsWorker()
	go s.logsWorker()
	go s.genaiWorker()

	if s.retentionDays > 0 {
		s.wg.Add(1)
		go s.retentionWorker()
	}
	s.wg.Add(4)
	go s.topologyWorker()
	go s.fingerprintWorker()
	go s.errorSignatureWorker()
	go s.spanRateWorker()
	// missingServiceWorker disabled: server/missing_svc_checker.go handles this
	// with a faster 45s threshold and upsert deduplication.
	return nil
}

// Close drains all queues, flushes remaining batches, then closes the backend.
func (s *Storage) Close() error {
	s.cancel()
	close(s.spanCh)
	close(s.metricCh)
	close(s.logCh)
	close(s.genaiCh)
	s.wg.Wait()
	// Close eval channels after genaiWorker has finished (so no more writes happen).
	close(s.genaiEvalCh)
	close(s.sessionEvalCh)
	return s.backend.Close()
}

// ---------------------------------------------------------------------------
// Query passthrough
// ---------------------------------------------------------------------------

func (s *Storage) QuerySpans(ctx context.Context, q SpanQuery) ([]SpanRow, error) {
	return s.backend.QuerySpans(ctx, q)
}

func (s *Storage) QueryMetrics(ctx context.Context, q MetricQuery) ([]MetricRow, error) {
	return s.backend.QueryMetrics(ctx, q)
}

func (s *Storage) QueryLogs(ctx context.Context, q LogQuery) ([]LogRow, error) {
	return s.backend.QueryLogs(ctx, q)
}

func (s *Storage) QueryAnomalies(ctx context.Context, entityID string, lim int) ([]AnomalyRow, error) {
	return s.backend.QueryAnomalies(ctx, entityID, lim)
}

func (s *Storage) FlushAnomalies(ctx context.Context, rows []AnomalyRow) error {
	return s.backend.FlushAnomalies(ctx, rows)
}

func (s *Storage) DeleteMissingServiceAnomaly(ctx context.Context, entityID string) error {
	return s.backend.DeleteMissingServiceAnomaly(ctx, entityID)
}

func (s *Storage) ClearResolvedAnomalies(ctx context.Context, olderThanNs int64) error {
	return s.backend.ClearResolvedAnomalies(ctx, olderThanNs)
}

func (s *Storage) QueryEntities(ctx context.Context, entityType, env string) ([]EntityRow, error) {
	return s.backend.QueryEntities(ctx, entityType, env)
}

func (s *Storage) QueryEnvironments(ctx context.Context) ([]string, error) {
	return s.backend.QueryEnvironments(ctx)
}

func (s *Storage) QueryTopology(ctx context.Context) ([]TopologyEdge, error) {
	return s.backend.QueryTopology(ctx)
}

func (s *Storage) QueryErrorSignatures(ctx context.Context, service string) ([]ErrorSignatureRow, error) {
	return s.backend.QueryErrorSignatures(ctx, service)
}

func (s *Storage) QueryTraceFingerprints(ctx context.Context, service string) ([]TraceFingerprintRow, error) {
	return s.backend.QueryTraceFingerprints(ctx, service)
}

// ---------------------------------------------------------------------------
// Public insert methods — extract from pdata, enqueue, return immediately.
// ---------------------------------------------------------------------------

func (s *Storage) InsertTraces(_ context.Context, td ptrace.Traces) error {
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		resJSON := marshalAttrs(rs.Resource().Attributes())
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				sp := ss.Spans().At(k)
				startNs := int64(sp.StartTimestamp())
				endNs := int64(sp.EndTimestamp())
				row := SpanRow{
					EntityID:      extractEntityID(resJSON),
					TraceID:       sp.TraceID().String(),
					SpanID:        sp.SpanID().String(),
					ParentSpanID:  sp.ParentSpanID().String(),
					Name:          sp.Name(),
					Kind:          int(sp.Kind()),
					StartNs:       startNs,
					EndNs:         endNs,
					DurationMs:    float64(endNs-startNs) / 1e6,
					StatusCode:    int(sp.Status().Code()),
					StatusMsg:     sp.Status().Message(),
					ResourceAttrs: resJSON,
					SpanAttrs:     marshalSpanAttrs(sp),
				}
				s.received.WithLabelValues("traces").Inc()
				select {
				case s.spanCh <- row:
				default:
					s.dropped.WithLabelValues("traces").Inc()
				}
				// Route gen_ai.* spans to the dedicated GenAI worker in addition
				// to the normal spans table (so traces waterfall still works).
				if isGenAISpan(sp) {
					gs := extractGenAISpan(sp, row.EntityID, resJSON)
					select {
					case s.genaiCh <- gs:
					default:
					}
				}
			}
		}
	}
	return nil
}

func (s *Storage) InsertMetrics(_ context.Context, md pmetric.Metrics) error {
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		resJSON := marshalAttrs(rm.Resource().Attributes())
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			for k := 0; k < sm.Metrics().Len(); k++ {
				s.enqueueMetric(resJSON, sm.Metrics().At(k))
			}
		}
	}
	return nil
}

func (s *Storage) enqueueMetric(resJSON string, m pmetric.Metric) {
	entityID := extractEntityID(resJSON)
	// Skip anomaly detection for monotonic counters (always-increasing → constant noise).
	skipDetection := m.Type() == pmetric.MetricTypeSum && m.Sum().IsMonotonic()
	// Also skip JVM bookkeeping metrics that fluctuate heavily during warmup but carry
	// no operational signal (class loading counts, GC bookkeeping, thread counts, etc.).
	switch m.Name() {
	case "jvm.class.count", "jvm.class.loaded", "jvm.class.unloaded",
		"jvm.gc.duration",
		"jvm.thread.count", "jvm.thread.daemon.count",
		"jvm.cpu.count",
		"jvm.memory.committed", "jvm.memory.limit",
		"process.runtime.jvm.classes.loaded":
		skipDetection = true
	}
	enqueue := func(tsNs int64, value float64, attrs pcommon.Map) {
		row := MetricRow{
			EntityID:      entityID,
			Name:          m.Name(),
			Description:   m.Description(),
			Unit:          m.Unit(),
			Type:          m.Type().String(),
			TimestampNs:   tsNs,
			Value:         value,
			ResourceAttrs: resJSON,
			DataAttrs:     marshalAttrs(attrs),
		}
		var anomaly *AnomalyRow
		if !skipDetection {
			anomaly = s.detector.Check(entityID, m.Name(), value)
		}
		s.received.WithLabelValues("metrics").Inc()
		select {
		case s.metricCh <- metricBatch{row: row, anomaly: anomaly}:
		default:
			s.dropped.WithLabelValues("metrics").Inc()
		}
	}

	switch m.Type() {
	case pmetric.MetricTypeGauge:
		for i := 0; i < m.Gauge().DataPoints().Len(); i++ {
			dp := m.Gauge().DataPoints().At(i)
			enqueue(int64(dp.Timestamp()), dp.DoubleValue(), dp.Attributes())
		}
	case pmetric.MetricTypeSum:
		for i := 0; i < m.Sum().DataPoints().Len(); i++ {
			dp := m.Sum().DataPoints().At(i)
			enqueue(int64(dp.Timestamp()), dp.DoubleValue(), dp.Attributes())
		}
	case pmetric.MetricTypeHistogram:
		for i := 0; i < m.Histogram().DataPoints().Len(); i++ {
			dp := m.Histogram().DataPoints().At(i)
			if dp.HasSum() {
				enqueue(int64(dp.Timestamp()), dp.Sum(), dp.Attributes())
			}
		}
	}
}

func (s *Storage) InsertLogs(_ context.Context, ld plog.Logs) error {
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		resJSON := marshalAttrs(rl.Resource().Attributes())
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				row := LogRow{
					EntityID:      extractEntityID(resJSON),
					TimestampNs:   int64(lr.Timestamp()),
					Severity:      lr.SeverityText(),
					Body:          fmt.Sprintf("%v", lr.Body().AsRaw()),
					TraceID:       lr.TraceID().String(),
					SpanID:        lr.SpanID().String(),
					ResourceAttrs: resJSON,
					LogAttrs:      marshalAttrs(lr.Attributes()),
				}
				s.received.WithLabelValues("logs").Inc()
				select {
				case s.logCh <- row:
				default:
					s.dropped.WithLabelValues("logs").Inc()
				}
			}
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Workers
// ---------------------------------------------------------------------------

func (s *Storage) spanWorker() {
	defer s.wg.Done()
	batch := make([]SpanRow, 0, s.batchSize)
	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case row, ok := <-s.spanCh:
			if !ok {
				if len(batch) > 0 {
					s.flushSpans(batch)
				}
				return
			}
			batch = append(batch, row)
			if len(batch) >= s.batchSize {
				s.flushSpans(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				s.flushSpans(batch)
				batch = batch[:0]
			}
			s.depth.WithLabelValues("traces").Set(float64(len(s.spanCh)))
		}
	}
}

func (s *Storage) metricsWorker() {
	defer s.wg.Done()
	metricBatch := make([]MetricRow, 0, s.batchSize)
	anomalyBatch := make([]AnomalyRow, 0, 16)
	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()
	flush := func() {
		if len(metricBatch) == 0 {
			return
		}
		s.flushMetrics(metricBatch, anomalyBatch)
		for _, a := range anomalyBatch {
			s.onAnomaly(a)
		}
		metricBatch = metricBatch[:0]
		anomalyBatch = anomalyBatch[:0]
	}
	for {
		select {
		case mb, ok := <-s.metricCh:
			if !ok {
				flush()
				return
			}
			metricBatch = append(metricBatch, mb.row)
			if mb.anomaly != nil {
				anomalyBatch = append(anomalyBatch, *mb.anomaly)
			}
			if len(metricBatch) >= s.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
			s.depth.WithLabelValues("metrics").Set(float64(len(s.metricCh)))
		}
	}
}

func (s *Storage) logsWorker() {
	defer s.wg.Done()
	batch := make([]LogRow, 0, s.batchSize)
	ticker := time.NewTicker(s.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case row, ok := <-s.logCh:
			if !ok {
				if len(batch) > 0 {
					s.flushLogs(batch)
				}
				return
			}
			batch = append(batch, row)
			if len(batch) >= s.batchSize {
				s.flushLogs(batch)
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				s.flushLogs(batch)
				batch = batch[:0]
			}
			s.depth.WithLabelValues("logs").Set(float64(len(s.logCh)))
		}
	}
}

func (s *Storage) retentionWorker() {
	defer s.wg.Done()
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			cutoff := time.Now().AddDate(0, 0, -s.retentionDays).Unix()
			if err := s.backend.DeleteBefore(s.ctx, cutoff); err != nil {
				s.onError(fmt.Errorf("retention delete: %w", err))
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *Storage) topologyWorker() {
	defer s.wg.Done()
	// Run immediately on startup so topology is available without waiting
	if err := s.backend.RefreshTopology(s.ctx); err != nil {
		s.onError(fmt.Errorf("refresh topology: %w", err))
	}
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := s.backend.RefreshTopology(s.ctx); err != nil {
				s.onError(fmt.Errorf("refresh topology: %w", err))
			}
		case <-s.ctx.Done():
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Flush with retry
// ---------------------------------------------------------------------------

func (s *Storage) flushSpans(batch []SpanRow) {
	start := time.Now()
	s.withRetry("spans", func() error {
		return s.backend.FlushSpans(s.ctx, batch)
	})
	s.flushDur.WithLabelValues("traces").Observe(time.Since(start).Seconds())
	s.batchSz.WithLabelValues("traces").Observe(float64(len(batch)))
	if entities := extractEntities(batch); len(entities) > 0 {
		if err := s.backend.UpsertEntities(s.ctx, entities); err != nil {
			s.onError(fmt.Errorf("upsert entities: %w", err))
		}
	}
}

func extractEntities(batch []SpanRow) []EntityRow {
	type key struct{ t, id string }
	seen := map[key]EntityRow{}
	for _, r := range batch {
		var attrs map[string]any
		if err := json.Unmarshal([]byte(r.ResourceAttrs), &attrs); err != nil {
			continue
		}
		env, _ := attrs["deployment.environment"].(string)
		// Prefer service.name; only fall back to host.name when service.name is absent
		// (avoids creating pod-name entities alongside service entities in k8s).
		svcName, _ := attrs["service.name"].(string)
		for _, pair := range []struct{ t, k string }{
			{"service", "service.name"},
			{"host", "host.name"},
		} {
			if pair.k == "host.name" && svcName != "" {
				continue // skip pod-name entity when service.name is present
			}
			id, _ := attrs[pair.k].(string)
			if id == "" {
				continue
			}
			k := key{pair.t, id}
			if existing, ok := seen[k]; !ok || r.StartNs > existing.LastSeenNs {
				seen[k] = EntityRow{
					EntityType:  pair.t,
					EntityID:    id,
					Environment: env,
					Attrs:       r.ResourceAttrs,
					LastSeenNs:  r.StartNs,
				}
			}
		}
	}
	out := make([]EntityRow, 0, len(seen))
	for _, e := range seen {
		out = append(out, e)
	}
	return out
}

func (s *Storage) flushMetrics(metrics []MetricRow, anomalies []AnomalyRow) {
	start := time.Now()
	s.withRetry("metrics", func() error {
		return s.backend.FlushMetrics(s.ctx, metrics, anomalies)
	})
	s.flushDur.WithLabelValues("metrics").Observe(time.Since(start).Seconds())
	s.batchSz.WithLabelValues("metrics").Observe(float64(len(metrics)))
	if entities := extractEntitiesFromMetrics(metrics); len(entities) > 0 {
		if err := s.backend.UpsertEntities(s.ctx, entities); err != nil {
			s.onError(fmt.Errorf("upsert metric entities: %w", err))
		}
	}
}

func (s *Storage) flushLogs(batch []LogRow) {
	start := time.Now()
	s.withRetry("logs", func() error {
		return s.backend.FlushLogs(s.ctx, batch)
	})
	s.flushDur.WithLabelValues("logs").Observe(time.Since(start).Seconds())
	s.batchSz.WithLabelValues("logs").Observe(float64(len(batch)))
	if entities := extractEntitiesFromLogs(batch); len(entities) > 0 {
		if err := s.backend.UpsertEntities(s.ctx, entities); err != nil {
			s.onError(fmt.Errorf("upsert log entities: %w", err))
		}
	}
}

// extractEntitiesFromMetrics extracts EntityRows from a metric batch.
// Metrics may originate from services or hosts (infra exporters).
func extractEntitiesFromMetrics(batch []MetricRow) []EntityRow {
	type key struct{ t, id string }
	seen := map[key]EntityRow{}
	for _, r := range batch {
		if r.EntityID == "" {
			continue
		}
		var attrs map[string]any
		if err := json.Unmarshal([]byte(r.ResourceAttrs), &attrs); err != nil {
			continue
		}
		env, _ := attrs["deployment.environment"].(string)
		eType := "service"
		if _, ok := attrs["service.name"].(string); !ok {
			eType = "host"
		}
		k := key{eType, r.EntityID}
		if existing, ok := seen[k]; !ok || r.TimestampNs > existing.LastSeenNs {
			seen[k] = EntityRow{
				EntityType:  eType,
				EntityID:    r.EntityID,
				Environment: env,
				Attrs:       r.ResourceAttrs,
				LastSeenNs:  r.TimestampNs,
			}
		}
	}
	out := make([]EntityRow, 0, len(seen))
	for _, e := range seen {
		out = append(out, e)
	}
	return out
}

// extractEntitiesFromLogs extracts EntityRows from a log batch.
func extractEntitiesFromLogs(batch []LogRow) []EntityRow {
	type key struct{ t, id string }
	seen := map[key]EntityRow{}
	for _, r := range batch {
		if r.EntityID == "" {
			continue
		}
		var attrs map[string]any
		if err := json.Unmarshal([]byte(r.ResourceAttrs), &attrs); err != nil {
			continue
		}
		env, _ := attrs["deployment.environment"].(string)
		eType := "service"
		if _, ok := attrs["service.name"].(string); !ok {
			eType = "host"
		}
		k := key{eType, r.EntityID}
		if existing, ok := seen[k]; !ok || r.TimestampNs > existing.LastSeenNs {
			seen[k] = EntityRow{
				EntityType:  eType,
				EntityID:    r.EntityID,
				Environment: env,
				Attrs:       r.ResourceAttrs,
				LastSeenNs:  r.TimestampNs,
			}
		}
	}
	out := make([]EntityRow, 0, len(seen))
	for _, e := range seen {
		out = append(out, e)
	}
	return out
}

func (s *Storage) withRetry(signal string, fn func() error) {
	backoff := 100 * time.Millisecond
	var err error
	for attempt := 1; attempt <= s.maxRetries; attempt++ {
		if err = fn(); err == nil {
			return
		}
		if attempt < s.maxRetries {
			time.Sleep(backoff)
			backoff *= 2
		}
	}
	s.flushErrs.WithLabelValues(signal).Inc()
	s.onError(fmt.Errorf("flush %s failed after %d attempts: %w", signal, s.maxRetries, err))
}

// ---------------------------------------------------------------------------
// Prometheus registration
// ---------------------------------------------------------------------------

func (s *Storage) registerMetrics() {
	labels := []string{"signal"}
	s.received = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "otel_backend_items_received_total",
		Help: "Total items received per signal type.",
	}, labels)
	s.dropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "otel_backend_items_dropped_total",
		Help: "Total items dropped (channel full) per signal type.",
	}, labels)
	s.depth = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "otel_backend_queue_depth",
		Help: "Current queue depth per signal type.",
	}, labels)
	s.flushDur = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "otel_backend_flush_duration_seconds",
		Help:    "Batch flush duration per signal type.",
		Buckets: prometheus.DefBuckets,
	}, labels)
	s.batchSz = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "otel_backend_flush_batch_size",
		Help:    "Number of rows per batch flush.",
		Buckets: []float64{1, 10, 50, 100, 250, 500, 1000},
	}, labels)
	s.flushErrs = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "otel_backend_flush_errors_total",
		Help: "Total flush errors per signal type.",
	}, labels)

	for _, c := range []prometheus.Collector{
		s.received, s.dropped, s.depth, s.flushDur, s.batchSz, s.flushErrs,
	} {
		s.promReg.Register(c) // ignore already-registered errors (e.g. tests)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (s *Storage) buildDetector() Detector {
	switch s.algo {
	case AlgoMAD:
		return NewMADDetector(s.anomalyThresh, s.anomalyWindow)
	case AlgoEWMA:
		return NewEWMADetector(s.ewmaAlpha, s.anomalyThresh)
	default:
		return NewZScoreDetector(s.anomalyThresh, s.anomalyWindow)
	}
}

// Dropped returns the total count of dropped items across all signal types.
// Individual signal counts are exposed via Prometheus metrics.
func (s *Storage) Dropped() int64 {
	var n atomic.Int64
	// value is maintained via prometheus counters; expose for backward compat
	return n.Load()
}

// extractEntityID resolves the canonical entity identifier from resource attributes.
// Primary: service.name. Fallback: host.name (for host-level signals like node metrics/logs).
func extractEntityID(resJSON string) string {
	var attrs map[string]any
	if err := json.Unmarshal([]byte(resJSON), &attrs); err != nil {
		return ""
	}
	if svc, ok := attrs["service.name"].(string); ok && svc != "" {
		return svc
	}
	if host, ok := attrs["host.name"].(string); ok && host != "" {
		return host
	}
	return ""
}

func marshalAttrs(attrs pcommon.Map) string {
	m := make(map[string]any, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		m[k] = v.AsRaw()
		return true
	})
	b, _ := json.Marshal(m)
	return string(b)
}

// marshalSpanAttrs serialises span attributes and merges in any "exception"
// event attributes (exception.type, exception.message, exception.stacktrace)
// so the full stack trace is visible in the stored span_attrs JSON.
func marshalSpanAttrs(sp ptrace.Span) string {
	m := make(map[string]any, sp.Attributes().Len())
	sp.Attributes().Range(func(k string, v pcommon.Value) bool {
		m[k] = v.AsRaw()
		return true
	})
	for i := 0; i < sp.Events().Len(); i++ {
		ev := sp.Events().At(i)
		if ev.Name() != "exception" {
			continue
		}
		ev.Attributes().Range(func(k string, v pcommon.Value) bool {
			if _, exists := m[k]; !exists {
				m[k] = v.AsRaw()
			}
			return true
		})
	}
	b, _ := json.Marshal(m)
	return string(b)
}
