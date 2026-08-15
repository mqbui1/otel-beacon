package server

// Cross-signal correlation engine.
//
// Watches the anomaly stream and fires a `correlated_incident` anomaly when
// the same entity accumulates 2+ distinct signal types within a 5-minute
// rolling window.  This gives high-confidence incident detection — a service
// showing both span_error_rate AND trace_drift simultaneously is much more
// likely to be a genuine incident than either signal alone.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/yourorg/otel-backend/storage"
)

const (
	correlatorWindow    = 5 * time.Minute  // look-back window for co-occurrence
	correlatorInterval  = 60 * time.Second // how often to run
	correlatorMinSignals = 2               // minimum distinct signal types for a correlated_incident
	correlatorSuppress  = 5 * time.Minute  // suppress re-fire for the same entity within this window
)

// StartCorrelator runs the cross-signal correlation engine in a background goroutine.
func StartCorrelator(ctx context.Context, store *storage.Storage, logger *zap.Logger) {
	go func() {
		var (
			mu              sync.Mutex
			lastFiredAt     = make(map[string]time.Time) // entity → last correlated_incident fire time
		)

		ticker := time.NewTicker(correlatorInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				byEntity, err := store.QueryRecentAnomaliesByEntity(ctx, int64(correlatorWindow.Seconds()))
				if err != nil {
					logger.Warn("correlator: query anomalies failed", zap.Error(err))
					continue
				}

				var toFlush []storage.AnomalyRow
				now := time.Now()

				mu.Lock()
				for entity, anomalies := range byEntity {
					// Gather distinct signal types for this entity (skip correlated_incident itself)
					sigSet := make(map[string]struct{})
					for _, a := range anomalies {
						if a.SignalType != "correlated_incident" {
							sigSet[a.SignalType] = struct{}{}
						}
					}
					if len(sigSet) < correlatorMinSignals {
						continue
					}

					// Suppress if we already fired recently for this entity
					if last, ok := lastFiredAt[entity]; ok && now.Sub(last) < correlatorSuppress {
						continue
					}

					// Determine escalated severity: critical if any contributing signal is critical
					severity := "warning"
					for _, a := range anomalies {
						if a.Severity == "critical" {
							severity = "critical"
							break
						}
					}

					sigs := make([]string, 0, len(sigSet))
					for s := range sigSet {
						sigs = append(sigs, s)
					}
					sort.Strings(sigs)

					lastFiredAt[entity] = now
					toFlush = append(toFlush, storage.AnomalyRow{
						EntityID:     entity,
						SignalType:   "correlated_incident",
						DetectorName: "cross_signal_correlator",
						MetricName:   "correlated_signals",
						Value:        float64(len(sigSet)),
						Score:        float64(len(sigSet)),
						Algorithm:    "correlation",
						Severity:     severity,
						Description:  fmt.Sprintf("Correlated incident on %s: %s (%d signal types co-occurring)", entity, strings.Join(sigs, " + "), len(sigSet)),
						DetectedAt:   now.UnixNano(),
					})
				}
				mu.Unlock()

				if len(toFlush) > 0 {
					if err := store.FlushAnomalies(ctx, toFlush); err != nil {
						logger.Warn("correlator: flush failed", zap.Error(err))
					} else {
						logger.Info("correlator: fired correlated incidents", zap.Int("count", len(toFlush)))
					}
				}
			}
		}
	}()
}
