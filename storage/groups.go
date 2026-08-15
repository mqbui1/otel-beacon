package storage

import "context"

// WriteMetrics directly flushes metrics to the backend, bypassing the ingestion queue.
// Used by background scrapers (e.g. k8s node metrics) that produce small batches on a timer.
func (s *Storage) WriteMetrics(ctx context.Context, metrics []MetricRow) error {
	return s.backend.FlushMetrics(ctx, metrics, nil)
}

func (s *Storage) UpsertIncidentGroup(ctx context.Context, g IncidentGroupRow) error {
	return s.backend.UpsertIncidentGroup(ctx, g)
}

func (s *Storage) QueryIncidentGroups(ctx context.Context, status string, limit int) ([]IncidentGroupRow, error) {
	return s.backend.QueryIncidentGroups(ctx, status, limit)
}

func (s *Storage) ResolveStaleIncidentGroups(ctx context.Context, staleSecs int64) error {
	return s.backend.ResolveStaleIncidentGroups(ctx, staleSecs)
}

func (s *Storage) SaveEntitySnapshot(ctx context.Context, snap EntitySnapshotRow) error {
	return s.backend.SaveEntitySnapshot(ctx, snap)
}

func (s *Storage) QueryEntitySnapshot(ctx context.Context, entityID string, nearNs int64) (*EntitySnapshotRow, error) {
	return s.backend.QueryEntitySnapshot(ctx, entityID, nearNs)
}
