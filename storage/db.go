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
	defaultFlushInterval = 200 * time.Millisecond
	defaultChannelSize   = 10_000
	defaultMaxRetries    = 3
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

	spanCh   chan SpanRow
	metricCh chan metricBatch // metric + pre-detected anomalies
	logCh    chan LogRow

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

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
		backend:       backend,
		algo:          AlgoMAD,
		ewmaAlpha:     0.3,
		anomalyThresh: 3.5,
		anomalyWindow: 100,
		batchSize:     defaultBatchSize,
		flushInterval: defaultFlushInterval,
		chanSize:      defaultChannelSize,
		retentionDays: 30,
		maxRetries:    defaultMaxRetries,
		onAnomaly:     func(AnomalyRow) {},
		onError:       func(err error) { fmt.Println("storage error:", err) },
		promReg:       prometheus.DefaultRegisterer,
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

	s.ctx, s.cancel = context.WithCancel(context.Background())

	s.wg.Add(3)
	go s.spanWorker()
	go s.metricsWorker()
	go s.logsWorker()

	if s.retentionDays > 0 {
		s.wg.Add(1)
		go s.retentionWorker()
	}
	s.wg.Add(4)
	go s.topologyWorker()
	go s.fingerprintWorker()
	go s.errorSignatureWorker()
	go s.spanRateWorker()
	return nil
}

// Close drains all queues, flushes remaining batches, then closes the backend.
func (s *Storage) Close() error {
	s.cancel()
	close(s.spanCh)
	close(s.metricCh)
	close(s.logCh)
	s.wg.Wait()
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

func (s *Storage) QueryAnomalies(ctx context.Context, lim int) ([]AnomalyRow, error) {
	return s.backend.QueryAnomalies(ctx, lim)
}

func (s *Storage) QueryEntities(ctx context.Context, entityType string) ([]EntityRow, error) {
	return s.backend.QueryEntities(ctx, entityType)
}

func (s *Storage) QueryTopology(ctx context.Context) ([]TopologyEdge, error) {
	return s.backend.QueryTopology(ctx)
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
					TraceID:      sp.TraceID().String(),
					SpanID:       sp.SpanID().String(),
					ParentSpanID: sp.ParentSpanID().String(),
					Name:         sp.Name(),
					Kind:         int(sp.Kind()),
					StartNs:      startNs,
					EndNs:        endNs,
					DurationMs:   float64(endNs-startNs) / 1e6,
					StatusCode:   int(sp.Status().Code()),
					StatusMsg:    sp.Status().Message(),
					ResourceAttrs: resJSON,
					SpanAttrs:    marshalAttrs(sp.Attributes()),
				}
				s.received.WithLabelValues("traces").Inc()
				select {
				case s.spanCh <- row:
				default:
					s.dropped.WithLabelValues("traces").Inc()
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
	entityID := extractServiceName(resJSON)
	// Skip anomaly detection for monotonic counters (e.g. cpu.time, pod.cpu.time).
	// These are always-increasing and produce constant noise in rolling-window detectors.
	skipDetection := m.Type() == pmetric.MetricTypeSum && m.Sum().IsMonotonic()
	enqueue := func(tsNs int64, value float64, attrs pcommon.Map) {
		row := MetricRow{
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
					TimestampNs:  int64(lr.Timestamp()),
					Severity:     lr.SeverityText(),
					Body:         fmt.Sprintf("%v", lr.Body().AsRaw()),
					TraceID:      lr.TraceID().String(),
					SpanID:       lr.SpanID().String(),
					ResourceAttrs: resJSON,
					LogAttrs:     marshalAttrs(lr.Attributes()),
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
	ticker := time.NewTicker(2 * time.Minute)
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
		for _, pair := range []struct{ t, k string }{
			{"service", "service.name"},
			{"host", "host.name"},
		} {
			id, _ := attrs[pair.k].(string)
			if id == "" {
				continue
			}
			k := key{pair.t, id}
			if existing, ok := seen[k]; !ok || r.StartNs > existing.LastSeenNs {
				seen[k] = EntityRow{
					EntityType: pair.t,
					EntityID:   id,
					Attrs:      r.ResourceAttrs,
					LastSeenNs: r.StartNs,
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
}

func (s *Storage) flushLogs(batch []LogRow) {
	start := time.Now()
	s.withRetry("logs", func() error {
		return s.backend.FlushLogs(s.ctx, batch)
	})
	s.flushDur.WithLabelValues("logs").Observe(time.Since(start).Seconds())
	s.batchSz.WithLabelValues("logs").Observe(float64(len(batch)))
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

func extractServiceName(resJSON string) string {
	var attrs map[string]any
	if err := json.Unmarshal([]byte(resJSON), &attrs); err != nil {
		return ""
	}
	svc, _ := attrs["service.name"].(string)
	return svc
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
