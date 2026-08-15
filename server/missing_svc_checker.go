package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/yourorg/otel-backend/storage"
)

const (
	missingSvcThreshold = 45 * time.Second
	missingSvcInterval  = 10 * time.Second
)

// missingSvcDenylist contains service names that are known infrastructure
// components which never emit root OTel spans themselves. They appear in
// span attributes (db.name, peer.service, etc.) and get registered as
// entities, but going silent is expected — not an incident.
var missingSvcDenylist = map[string]bool{
	// Generic infra that commonly appears as span peers
	"eureka":           true,
	"zipkin":           true,
	"jaeger":           true,
	"prometheus":       true,
	"grafana":          true,
	"loki":             true,
	"otel-collector":   true,
	"otelcol-contrib":  true,
	// Astronomy shop infra — passive services, not expected to emit root spans
	"postgresql":       true,
	"valkey-cart":      true,
	"kafka":            true,
	"flagd":            true,
	// k3d / Kubernetes system components — one-shot jobs or cluster infra,
	// going silent is expected and should never trigger incidents.
	"helm-install-traefik":     true,
	"helm-install-traefik-crd": true,
	"traefik":                  true,
	"traefik-kube-system":      true,
	"coredns":                  true,
	"metrics-server":           true,
	"local-path-provisioner":   true,
	"astronomy-shop":           true, // helm release meta-entity
	"load-generator":           true, // synthetic load gen, not a real service
	// Transient scenario-injected services — appear only during anomaly phase,
	// going silent after cooldown is expected and should not trigger incidents.
	"audit-service":    true, // new_call_path: injected new dependency
	"accounting":       true, // payment_timeout: external payment via topology
	// Astronomy shop simulator services — only exist while the simulator runs.
	// Going silent after a scenario ends is expected and should not trigger incidents.
	"checkout":         true,
	"recommendation":   true,
	"product-catalog":  true,
	"payment":          true,
	"frontend-proxy":   true,
}

// StartMissingSvcChecker periodically checks whether any known service entity
// has gone silent (no spans/metrics/logs for > missingSvcThreshold) and inserts
// a missing_service anomaly so the topology node turns gray.
// When the service resumes reporting, the anomaly is deleted automatically.
func StartMissingSvcChecker(ctx context.Context, store *storage.Storage, logger *zap.Logger) {
	go func() {
		ticker := time.NewTicker(missingSvcInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runMissingSvcCheck(ctx, store, logger)
			}
		}
	}()
	logger.Info("missing service checker started",
		zap.Duration("threshold", missingSvcThreshold),
		zap.Duration("interval", missingSvcInterval))
}

func runMissingSvcCheck(ctx context.Context, store *storage.Storage, logger *zap.Logger) {
	entities, err := store.QueryEntities(ctx, "service", "")
	if err != nil {
		logger.Warn("missing svc check: query entities", zap.Error(err))
		return
	}

	now := time.Now().UnixNano()
	cutoff := now - missingSvcThreshold.Nanoseconds()

	for _, e := range entities {
		if e.LastSeenNs == 0 {
			continue
		}
		if missingSvcDenylist[e.EntityID] ||
			strings.HasPrefix(e.EntityID, "svclb-") ||
			strings.HasPrefix(e.EntityID, "kube-") ||
			strings.HasPrefix(e.EntityID, "helm-install-") {
			_ = store.DeleteMissingServiceAnomaly(ctx, e.EntityID)
			continue
		}
		if e.LastSeenNs < cutoff {
			silentSec := float64(now-e.LastSeenNs) / 1e9
			// Delete the existing row first so we upsert a single up-to-date record
			// rather than accumulating one row per check interval.
			_ = store.DeleteMissingServiceAnomaly(ctx, e.EntityID)
			anomaly := storage.AnomalyRow{
				EntityID:     e.EntityID,
				SignalType:   "missing_service",
				DetectorName: "staleness",
				MetricName:   "last_seen_ns",
				Value:        silentSec,
				Score:        silentSec / missingSvcThreshold.Seconds(),
				Algorithm:    "threshold",
				Severity:     "critical",
				Description:  fmt.Sprintf("%s has not reported for %.0fs", e.EntityID, silentSec),
				DetectedAt:   now,
			}
			if err := store.FlushAnomalies(ctx, []storage.AnomalyRow{anomaly}); err != nil {
				logger.Warn("missing svc: flush anomaly", zap.Error(err))
			}
		} else {
			// Service is active — remove any stale missing_service anomaly so the
			// topology node returns to its normal health color immediately.
			if err := store.DeleteMissingServiceAnomaly(ctx, e.EntityID); err != nil {
				logger.Warn("missing svc: delete anomaly", zap.Error(err))
			}
		}
	}
}
