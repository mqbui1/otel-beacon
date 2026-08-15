package server

// Topology-aware incident grouping engine.
//
// Runs every 90 seconds. Finds entities that have simultaneous active anomalies
// AND are connected in the service topology graph. Groups them into IncidentGroups
// with an inferred root-cause entity (upstream-most or earliest-firing).
//
// This is analogous to Ciroos Signal Intelligence™ — collapsing alert storms
// into a single actionable investigation rather than N separate incident cards.

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/yourorg/otel-backend/storage"
)

const (
	grouperInterval = 90 * time.Second
	grouperWindow   = int64(30 * 60) // anomalies within last 30 min are "active" (matches resolvedThresholdNs)
	grouperStale    = int64(30 * 60) // resolve groups with no new signals in 30 min (keeps one-shot signals visible)
)

// StartGrouper runs the topology-aware incident grouping engine in a background goroutine.
func StartGrouper(ctx context.Context, store *storage.Storage, logger *zap.Logger) {
	go func() {
		ticker := time.NewTicker(grouperInterval)
		defer ticker.Stop()
		// Run immediately on start to clean up stale groups fast.
		runGrouper(ctx, store, logger)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runGrouper(ctx, store, logger)
			}
		}
	}()
}

func runGrouper(ctx context.Context, store *storage.Storage, logger *zap.Logger) {
	// 1. Resolve groups that haven't had new signals in grouperStale seconds.
	_ = store.ResolveStaleIncidentGroups(ctx, grouperStale)

	// 2. Get all recent anomalies grouped by entity.
	byEntity, err := store.QueryRecentAnomaliesByEntity(ctx, grouperWindow)
	if err != nil {
		logger.Warn("grouper: query anomalies failed", zap.Error(err))
		return
	}
	if len(byEntity) == 0 {
		return
	}

	// Remove entities that only have metric-type anomalies — same filter as the
	// incidents handler so pure-infra noise (k8s nodes, kafka, etc.) doesn't
	// create incident groups.
	for eid, rows := range byEntity {
		hasNonMetric := false
		for _, a := range rows {
			if a.SignalType != "metric" {
				hasNonMetric = true
				break
			}
		}
		if !hasNonMetric {
			delete(byEntity, eid)
		}
	}
	if len(byEntity) == 0 {
		return
	}

	// 3. Get topology to find connected services.
	edges, _ := store.QueryTopology(ctx)

	// 4. BFS to find connected components among entities with active anomalies.
	visited := make(map[string]bool)
	var components [][]string
	for entity := range byEntity {
		if visited[entity] {
			continue
		}
		comp := grouperBFS(entity, byEntity, edges, visited)
		if len(comp) > 0 {
			components = append(components, comp)
		}
	}

	// 5. Upsert one IncidentGroupRow per component.
	for _, comp := range components {
		sort.Strings(comp)
		rootEntity := findGroupRoot(comp, byEntity, edges)

		severity := "warning"
		sigSet := make(map[string]struct{})
		var firstNs, lastNs int64

		// Signal types that can drive "critical" severity — must reflect real service impact.
		// Pure metric anomalies (even with high z-scores) are never critical on their own.
		criticalSignals := map[string]bool{
			"span_error_rate": true, "span_latency": true,
			"missing_service": true, "error_signature": true,
			"correlated_incident": false, // correlator already upgraded based on co-occurrence
		}

		for _, eid := range comp {
			for _, a := range byEntity[eid] {
				if a.SignalType == "correlated_incident" {
					continue
				}
				if a.Severity == "critical" && criticalSignals[a.SignalType] {
					severity = "critical"
				}
				sigSet[a.SignalType] = struct{}{}
				if firstNs == 0 || a.DetectedAt < firstNs {
					firstNs = a.DetectedAt
				}
				if a.DetectedAt > lastNs {
					lastNs = a.DetectedAt
				}
			}
		}
		if firstNs == 0 {
			firstNs = time.Now().UnixNano()
		}
		if lastNs == 0 {
			lastNs = firstNs
		}

		sigs := make([]string, 0, len(sigSet))
		for s := range sigSet {
			sigs = append(sigs, s)
		}
		sort.Strings(sigs)

		if rootEntity == "" {
			continue
		}
		// Deterministic group ID: root entity + 30-minute bucket based on current time.
		// Using now (not firstNs) ensures the same incident maps to the same group
		// throughout a 30-minute window instead of creating a new group each rotation.
		bucket := time.Now().UnixNano() / (30 * 60 * 1_000_000_000)
		groupID := fmt.Sprintf("%s-%d", rootEntity, bucket)

		affected, _ := json.Marshal(comp)
		sigsJSON, _ := json.Marshal(sigs)

		g := storage.IncidentGroupRow{
			GroupID:          groupID,
			RootEntityID:     rootEntity,
			AffectedEntities: string(affected),
			Severity:         severity,
			Status:           "active",
			SignalTypes:      string(sigsJSON),
			Description:      buildGroupDesc(rootEntity, comp, sigs, severity),
			FirstSeenNs:      firstNs,
			LastSeenNs:       lastNs,
		}
		if err := store.UpsertIncidentGroup(ctx, g); err != nil {
			logger.Warn("grouper: upsert failed", zap.Error(err), zap.String("group", groupID))
		}
	}

	if len(components) > 0 {
		logger.Info("grouper: updated incident groups", zap.Int("groups", len(components)))
	}
}

// grouperBFS walks the topology graph, collecting connected entities that all have active anomalies.
func grouperBFS(start string, byEntity map[string][]storage.AnomalyRow, edges []storage.TopologyEdge, visited map[string]bool) []string {
	var comp []string
	queue := []string{start}
	for len(queue) > 0 {
		curr := queue[0]
		queue = queue[1:]
		if visited[curr] {
			continue
		}
		visited[curr] = true
		comp = append(comp, curr)
		for _, e := range edges {
			if e.SourceService == curr {
				if _, ok := byEntity[e.TargetService]; ok && !visited[e.TargetService] {
					queue = append(queue, e.TargetService)
				}
			}
			if e.TargetService == curr {
				if _, ok := byEntity[e.SourceService]; ok && !visited[e.SourceService] {
					queue = append(queue, e.SourceService)
				}
			}
		}
	}
	return comp
}

// findGroupRoot identifies the root entity in a group using topology position:
// an entity with many downstream connections and few incoming = closer to root.
// Tie-break: earliest anomaly detected.
func findGroupRoot(comp []string, byEntity map[string][]storage.AnomalyRow, edges []storage.TopologyEdge) string {
	compSet := make(map[string]bool, len(comp))
	for _, e := range comp {
		compSet[e] = true
	}
	outScore := make(map[string]int, len(comp))
	inScore := make(map[string]int, len(comp))
	for _, e := range edges {
		if compSet[e.SourceService] && compSet[e.TargetService] {
			outScore[e.SourceService]++
			inScore[e.TargetService]++
		}
	}

	bestEntity := comp[0]
	bestNet := -999
	var bestEarliestNs int64

	for _, eid := range comp {
		net := outScore[eid] - inScore[eid]
		var earliestNs int64
		for _, a := range byEntity[eid] {
			if a.SignalType == "correlated_incident" {
				continue
			}
			if earliestNs == 0 || a.DetectedAt < earliestNs {
				earliestNs = a.DetectedAt
			}
		}
		better := net > bestNet
		if net == bestNet && bestEarliestNs > 0 && earliestNs > 0 && earliestNs < bestEarliestNs {
			better = true
		}
		if better {
			bestNet = net
			bestEntity = eid
			bestEarliestNs = earliestNs
		}
	}
	return bestEntity
}

func buildGroupDesc(root string, affected []string, sigs []string, severity string) string {
	sigStr := strings.Join(sigs, "+")
	if len(affected) == 1 {
		return fmt.Sprintf("[%s] %s: %s", severity, root, sigStr)
	}
	others := make([]string, 0, len(affected)-1)
	for _, a := range affected {
		if a != root {
			others = append(others, a)
		}
	}
	sort.Strings(others)
	return fmt.Sprintf("[%s] %s cascaded to %s (%s)", severity, root, strings.Join(others, ", "), sigStr)
}
