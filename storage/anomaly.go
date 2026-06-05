package storage

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"time"
)

// Detector is the anomaly detection interface. Implementations are ZScore, MAD, and EWMA.
// entity is the service/host entity ID; name is the metric name.
// Returns nil if no anomaly, or an AnomalyRow if threshold exceeded.
type Detector interface {
	Check(entity, name string, value float64) *AnomalyRow
}

const anomalyCooldown = 5 * time.Minute

func entityKey(entity, name string) string { return entity + "\x00" + name }

func scoreSeverity(score, threshold float64) string {
	if score >= threshold*2 {
		return "critical"
	}
	return "warning"
}

// ---------------------------------------------------------------------------
// Z-Score (rolling window, simple baseline)
// ---------------------------------------------------------------------------

type ZScoreDetector struct {
	threshold  float64
	windowSize int
	mu         sync.Mutex
	windows    map[string][]float64
	lastFired  map[string]time.Time
}

func NewZScoreDetector(threshold float64, windowSize int) *ZScoreDetector {
	return &ZScoreDetector{
		threshold:  threshold,
		windowSize: windowSize,
		windows:    make(map[string][]float64),
		lastFired:  make(map[string]time.Time),
	}
}

func (d *ZScoreDetector) Check(entity, name string, value float64) *AnomalyRow {
	key := entityKey(entity, name)
	d.mu.Lock()
	defer d.mu.Unlock()
	win := appendWindow(d.windows[key], value, d.windowSize)
	d.windows[key] = win
	if len(win) < 10 {
		return nil
	}
	mean, stddev := meanStddev(win)
	if stddev == 0 {
		return nil
	}
	z := math.Abs(value-mean) / stddev
	if z <= d.threshold {
		return nil
	}
	if t, ok := d.lastFired[key]; ok && time.Since(t) < anomalyCooldown {
		return nil
	}
	d.lastFired[key] = time.Now()
	return anomalyRow(entity, name, value, z, mean, stddev, "zscore", d.threshold)
}

// ---------------------------------------------------------------------------
// MAD — Median Absolute Deviation (robust to outliers)
// Uses the modified Z-score: 0.6745 * |x - median| / MAD
// Recommended threshold: 3.5
// ---------------------------------------------------------------------------

type MADDetector struct {
	threshold  float64
	windowSize int
	mu         sync.Mutex
	windows    map[string][]float64
	lastFired  map[string]time.Time
}

func NewMADDetector(threshold float64, windowSize int) *MADDetector {
	return &MADDetector{
		threshold:  threshold,
		windowSize: windowSize,
		windows:    make(map[string][]float64),
		lastFired:  make(map[string]time.Time),
	}
}

func (d *MADDetector) Check(entity, name string, value float64) *AnomalyRow {
	key := entityKey(entity, name)
	d.mu.Lock()
	defer d.mu.Unlock()
	win := appendWindow(d.windows[key], value, d.windowSize)
	d.windows[key] = win
	if len(win) < 10 {
		return nil
	}
	med := median(win)
	devs := make([]float64, len(win))
	for i, v := range win {
		devs[i] = math.Abs(v - med)
	}
	mad := median(devs)
	if mad == 0 {
		return nil
	}
	// Modified Z-score (Iglewicz & Hoaglin)
	score := 0.6745 * math.Abs(value-med) / mad
	if score <= d.threshold {
		return nil
	}
	if t, ok := d.lastFired[key]; ok && time.Since(t) < anomalyCooldown {
		return nil
	}
	d.lastFired[key] = time.Now()
	return anomalyRow(entity, name, value, score, med, mad, "mad", d.threshold)
}

// ---------------------------------------------------------------------------
// EWMA — Exponentially Weighted Moving Average
// Adapts quickly to trends; alpha controls recency weight (0 < alpha < 1).
// Higher alpha = more weight on recent values. Default: 0.3
// Recommended threshold: 3.0
// ---------------------------------------------------------------------------

type EWMADetector struct {
	alpha     float64
	threshold float64
	mu        sync.Mutex
	state     map[string]*ewmaState
	lastFired map[string]time.Time
}

type ewmaState struct {
	mean     float64
	variance float64
	ready    bool
}

func NewEWMADetector(alpha, threshold float64) *EWMADetector {
	return &EWMADetector{
		alpha:     alpha,
		threshold: threshold,
		state:     make(map[string]*ewmaState),
		lastFired: make(map[string]time.Time),
	}
}

func (d *EWMADetector) Check(entity, name string, value float64) *AnomalyRow {
	key := entityKey(entity, name)
	d.mu.Lock()
	defer d.mu.Unlock()
	s, ok := d.state[key]
	if !ok {
		s = &ewmaState{}
		d.state[key] = s
	}
	if !s.ready {
		s.mean = value
		s.variance = 0
		s.ready = true
		return nil
	}
	diff := value - s.mean
	s.mean += d.alpha * diff
	s.variance = (1 - d.alpha) * (s.variance + d.alpha*diff*diff)
	stddev := math.Sqrt(s.variance)
	if stddev == 0 {
		return nil
	}
	z := math.Abs(diff) / stddev
	if z <= d.threshold {
		return nil
	}
	if t, ok := d.lastFired[key]; ok && time.Since(t) < anomalyCooldown {
		return nil
	}
	d.lastFired[key] = time.Now()
	return anomalyRow(entity, name, value, z, s.mean, stddev, "ewma", d.threshold)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func appendWindow(win []float64, value float64, maxSize int) []float64 {
	win = append(win, value)
	if len(win) > maxSize {
		win = win[len(win)-maxSize:]
	}
	return win
}

func anomalyRow(entity, name string, value, score, mean, stddev float64, algo string, threshold float64) *AnomalyRow {
	return &AnomalyRow{
		EntityID:     entity,
		SignalType:   "metric",
		DetectorName: "Metric Anomaly",
		MetricName:   name,
		Value:        value,
		Score:        score,
		Mean:         mean,
		StdDev:       stddev,
		Algorithm:    algo,
		Severity:     scoreSeverity(score, threshold),
		Description:  fmt.Sprintf("%s: value %.4g deviates from mean %.4g (score %.2f)", name, value, mean, score),
		DetectedAt:   time.Now().Unix(),
	}
}

func meanStddev(values []float64) (mean, stddev float64) {
	n := float64(len(values))
	for _, v := range values {
		mean += v
	}
	mean /= n
	for _, v := range values {
		d := v - mean
		stddev += d * d
	}
	stddev = math.Sqrt(stddev / n)
	return
}

func median(values []float64) float64 {
	cp := make([]float64, len(values))
	copy(cp, values)
	sort.Float64s(cp)
	n := len(cp)
	if n%2 == 0 {
		return (cp[n/2-1] + cp[n/2]) / 2
	}
	return cp[n/2]
}
