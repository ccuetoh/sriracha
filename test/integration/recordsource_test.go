//go:build integration

package integration_test

import (
	"context"
	"sort"

	"go.sriracha.dev/sriracha"
)

// memRecordSource is a deterministic, map-backed sriracha.RecordSource. Scan
// iterates record IDs in sorted order so that an Indexer.Rebuild produces the
// same key ordering across runs (required for stable Bloom score thresholds
// in the integration suite).
type memRecordSource struct {
	records map[string]sriracha.RawRecord
}

func newMemRecordSource(records map[string]sriracha.RawRecord) *memRecordSource {
	return &memRecordSource{records: records}
}

func (s *memRecordSource) Scan(ctx context.Context, fn func(id string, r sriracha.RawRecord) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ids := make([]string, 0, len(s.records))
	for id := range s.records {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(id, s.records[id]); err != nil {
			return err
		}
	}
	return nil
}

func (s *memRecordSource) Fetch(ctx context.Context, id string) (sriracha.RawRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r, ok := s.records[id]
	if !ok {
		return nil, sriracha.ErrRecordNotFound(id)
	}
	return r, nil
}
