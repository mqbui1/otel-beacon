package storage

import "context"

// Storage pass-throughs for change record ingestion.

func (s *Storage) InsertChangeEvent(ctx context.Context, e ChangeEventRow) (int64, error) {
	return s.backend.InsertChangeEvent(ctx, e)
}

func (s *Storage) QueryChangeEvents(ctx context.Context, entityID string, fromSecs, toSecs int64, limit int) ([]ChangeEventRow, error) {
	return s.backend.QueryChangeEvents(ctx, entityID, fromSecs, toSecs, limit)
}
