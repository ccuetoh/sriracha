package indexer

import (
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"go.sriracha.dev/fieldset"
	"go.sriracha.dev/internal/bitset"
	"go.sriracha.dev/sriracha"
	"go.sriracha.dev/token"
)

const (
	EntryPrefixDeterministic = "det:"
	EntryPrefixProbabilistic = "prob:"
	EntryPrefixMeta          = "meta:"
	EntryPrefixStats         = "stats:"

	DefaultThreshold  = 0.85
	DefaultMaxResults = 1

	StatsVersion = "v1"
)

// StorageSizer is an optional interface for IndexStorage implementations
// that can report their current storage footprint.
type StorageSizer interface {
	SizeBytes(ctx context.Context) (int64, error)
}

// Transactor is an optional interface for IndexStorage backends that support
// atomic multi-key writes and deletes in a single operation.
type Transactor interface {
	PutBatch(ctx context.Context, kvs map[string][]byte) error
	DeleteBatch(ctx context.Context, keys []string) error
}

type storedMeta struct {
	DetKey  string `json:"det_key"`
	ProbKey string `json:"prob_key"`
}

type persistedStats struct {
	RecordCount int64     `json:"record_count"`
	LastRebuild time.Time `json:"last_rebuild"`
	LastSync    time.Time `json:"last_sync"`
}

// Indexer implements sriracha.TokenIndexer backed by any IndexStorage.
type Indexer struct {
	tok     *token.Tokenizer
	fs      sriracha.FieldSet
	storage sriracha.IndexStorage
	stats   sriracha.IndexStats
}

// New constructs an Indexer backed by the given storage.
// Any sriracha.IndexStorage implementation may be provided.
func New(storage sriracha.IndexStorage, fs sriracha.FieldSet, secret []byte) (*Indexer, error) {
	if storage == nil {
		return nil, sriracha.ErrInternalError("storage must not be nil")
	}

	tok, err := token.New(secret)
	if err != nil {
		return nil, err
	}

	if err := fieldset.Validate(fs); err != nil {
		return nil, err
	}

	idx := &Indexer{tok: tok, fs: fs, storage: storage}
	if err := idx.loadStats(context.Background()); err != nil {
		return nil, err
	}

	return idx, nil
}

// NewDefault opens a persistent Badger store at dir and constructs an Indexer.
// It is a convenience wrapper around OpenBadger and New.
func NewDefault(dir string, fs sriracha.FieldSet, secret []byte) (*Indexer, error) {
	s, err := OpenBadger(dir)
	if err != nil {
		return nil, err
	}

	idx, err := New(s, fs, secret)
	if err != nil {
		_ = s.Close()
		return nil, err
	}

	return idx, nil
}

// Close releases the underlying storage if it implements io.Closer.
func (idx *Indexer) Close() error {
	if c, ok := idx.storage.(io.Closer); ok {
		return c.Close()
	}

	return nil
}

// Stats returns current index statistics.
// If the storage implements StorageSizer, IndexSizeBytes is populated.
func (idx *Indexer) Stats() sriracha.IndexStats {
	if sizer, ok := idx.storage.(StorageSizer); ok {
		bytes, _ := sizer.SizeBytes(context.Background())
		idx.stats.IndexSizeBytes = bytes
	}

	return idx.stats
}

// Rebuild reindexes all records from src, clearing the existing index first.
func (idx *Indexer) Rebuild(ctx context.Context, src sriracha.RecordSource) error {
	var toDelete []string
	for _, prefix := range []string{
		EntryPrefixDeterministic,
		EntryPrefixProbabilistic,
		EntryPrefixMeta,
	} {
		if err := idx.storage.Scan(ctx, prefix, func(key string, _ []byte) error {
			toDelete = append(toDelete, key)
			return nil
		}); err != nil {
			return err
		}
	}

	if tx, ok := idx.storage.(Transactor); ok {
		if err := tx.DeleteBatch(ctx, toDelete); err != nil {
			return err
		}
	} else {
		for _, key := range toDelete {
			if err := idx.storage.Delete(ctx, key); err != nil {
				return err
			}
		}
	}

	var counter int64
	if err := src.Scan(ctx, func(id string, r sriracha.RawRecord) error {
		if err := idx.indexRecord(ctx, id, r); err != nil {
			return err
		}

		counter++
		return nil
	}); err != nil {
		return err
	}

	idx.stats.RecordCount = counter
	idx.stats.LastRebuild = time.Now()
	return idx.saveStats(ctx)
}

// Sync applies incremental updates from src since the last checkpoint.
func (idx *Indexer) Sync(ctx context.Context, src sriracha.IncrementalRecordSource) error {
	checkpoint, err := idx.storage.LoadCheckpoint(ctx)
	if err != nil {
		return err
	}

	if err := src.ScanSince(ctx, checkpoint, func(id string, r sriracha.RawRecord) error {
		if len(r) == 0 {
			if err := idx.deleteRecord(ctx, id); err != nil {
				return err
			}

			idx.stats.RecordCount--
			return nil
		}

		if err := idx.deleteRecord(ctx, id); err != nil {
			return err
		}

		if err := idx.indexRecord(ctx, id, r); err != nil {
			return err
		}

		idx.stats.RecordCount++
		return nil
	}); err != nil {
		return err
	}

	if err := idx.storage.SaveCheckpoint(ctx, time.Now().UTC().Format(time.RFC3339)); err != nil {
		return err
	}

	idx.stats.LastSync = time.Now()
	return idx.saveStats(ctx)
}

// Match searches the index for candidates matching tr under cfg.
func (idx *Indexer) Match(ctx context.Context, tr sriracha.TokenRecord, cfg sriracha.MatchConfig) ([]sriracha.Candidate, error) {
	cfg.Threshold = cmp.Or(cfg.Threshold, DefaultThreshold)
	cfg.MaxResults = cmp.Or(cfg.MaxResults, DefaultMaxResults)

	if cfg.MaxResults == 0 {
		cfg.MaxResults = 1
	}

	if tr.FieldSetVersion != idx.fs.Version {
		return nil, sriracha.ErrFieldSetIncompatible(tr.FieldSetVersion, idx.fs.Version)
	}

	switch tr.Mode {
	case sriracha.Deterministic:
		return idx.matchDeterministic(ctx, tr)
	case sriracha.Probabilistic:
		return idx.matchProbabilistic(ctx, tr, cfg)
	default:
		return nil, sriracha.ErrInternalError(fmt.Sprintf("unknown match mode %d", tr.Mode))
	}
}

func (idx *Indexer) matchDeterministic(ctx context.Context, tr sriracha.TokenRecord) ([]sriracha.Candidate, error) {
	val, err := idx.storage.Get(ctx, EntryPrefixDeterministic+hex.EncodeToString(tr.Payload))
	if err != nil {
		if errors.Is(err, sriracha.ErrNotFound) {
			return nil, nil
		}

		return nil, err
	}

	return []sriracha.Candidate{{RecordID: string(val), Confidence: 1.0}}, nil
}

func (idx *Indexer) matchProbabilistic(ctx context.Context, tr sriracha.TokenRecord, cfg sriracha.MatchConfig) ([]sriracha.Candidate, error) {
	fieldBytes := fieldFilterBytes(idx.fs.BloomParams.SizeBits)
	expectedLen := len(idx.fs.Fields) * fieldBytes
	if len(tr.Payload) != expectedLen {
		return nil, sriracha.ErrIndexCorrupted("query payload length mismatch")
	}

	scanPrefix := EntryPrefixProbabilistic + idx.fs.Version + ":"
	var candidates []sriracha.Candidate

	if err := idx.storage.Scan(ctx, scanPrefix, func(key string, storedPayload []byte) error {
		if len(storedPayload) != len(tr.Payload) {
			return sriracha.ErrIndexCorrupted("stored payload length mismatch")
		}

		recordID := key[len(scanPrefix):]
		conf, err := scoreProbabilistic(tr.Payload, storedPayload, idx.fs, cfg)
		if err != nil {
			return err
		}

		if conf >= float64(cfg.Threshold) {
			candidates = append(candidates, sriracha.Candidate{RecordID: recordID, Confidence: conf})
		}

		return nil
	}); err != nil {
		return nil, err
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Confidence > candidates[j].Confidence
	})

	if int(cfg.MaxResults) < len(candidates) {
		candidates = candidates[:cfg.MaxResults]
	}

	return candidates, nil
}

func scoreProbabilistic(query, stored []byte, fs sriracha.FieldSet, cfg sriracha.MatchConfig) (float64, error) {
	fieldBytes := fieldFilterBytes(fs.BloomParams.SizeBits)
	expectedLen := len(fs.Fields) * fieldBytes
	if len(query) != expectedLen || len(stored) != expectedLen {
		return 0, sriracha.ErrIndexCorrupted("payload length mismatch in scorer")
	}

	var numerator, denominator float64
	for i, spec := range fs.Fields {
		start := i * fieldBytes
		end := start + fieldBytes

		bsQ, err := bitset.FromBytes(query[start:end])
		if err != nil {
			return 0, err
		}

		bsS, err := bitset.FromBytes(stored[start:end])
		if err != nil {
			return 0, err
		}

		popQ := bitset.Popcount(bsQ)
		popS := bitset.Popcount(bsS)
		if popQ == 0 && popS == 0 {
			continue
		}

		inter, err := bitset.And(bsQ, bsS)
		if err != nil {
			return 0, err
		}

		popInter := bitset.Popcount(inter)
		dice := (2.0 * float64(popInter)) / float64(popQ+popS)

		w := fieldWeightFor(i, spec, cfg)
		numerator += w * dice
		denominator += w
	}

	if denominator == 0 {
		return 0, nil
	}

	return numerator / denominator, nil
}

func fieldWeightFor(_ int, spec sriracha.FieldSpec, cfg sriracha.MatchConfig) float64 {
	for _, fw := range cfg.FieldWeights {
		if fw.Path == spec.Path {
			return fw.Weight
		}
	}
	return spec.Weight
}

// fieldFilterBytes returns the byte length of one Bloom filter field slice,
// matching the output of bitset.New(n).ToBytes(): ceil(n/64)*8 bytes.
func fieldFilterBytes(sizeBits uint32) int {
	return int(((sizeBits + 63) / 64) * 8)
}

func (idx *Indexer) indexRecord(ctx context.Context, id string, r sriracha.RawRecord) error {
	detTR, err := idx.tok.TokenizeRecord(r, idx.fs)
	if err != nil {
		return err
	}

	probTR, err := idx.tok.TokenizeRecordBloom(r, idx.fs)
	if err != nil {
		return err
	}

	detKey := EntryPrefixDeterministic + hex.EncodeToString(detTR.Payload)
	probKey := EntryPrefixProbabilistic + idx.fs.Version + ":" + id
	metaKey := EntryPrefixMeta + id

	meta := storedMeta{DetKey: detKey, ProbKey: probKey}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return err
	}

	if tx, ok := idx.storage.(Transactor); ok {
		return tx.PutBatch(ctx, map[string][]byte{
			detKey:  []byte(id),
			probKey: probTR.Payload,
			metaKey: metaBytes,
		})
	}

	return errors.Join(
		idx.storage.Put(ctx, detKey, []byte(id)),
		idx.storage.Put(ctx, probKey, probTR.Payload),
		idx.storage.Put(ctx, metaKey, metaBytes),
	)
}

func (idx *Indexer) deleteRecord(ctx context.Context, id string) error {
	metaKey := EntryPrefixMeta + id
	metaBytes, err := idx.storage.Get(ctx, metaKey)
	if err != nil {
		if errors.Is(err, sriracha.ErrNotFound) {
			return nil
		}
		return err
	}

	var meta storedMeta
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return err
	}

	if tx, ok := idx.storage.(Transactor); ok {
		return tx.DeleteBatch(ctx, []string{meta.DetKey, meta.ProbKey, metaKey})
	}

	return errors.Join(
		idx.storage.Delete(ctx, meta.DetKey),
		idx.storage.Delete(ctx, meta.ProbKey),
		idx.storage.Delete(ctx, metaKey),
	)
}

func (idx *Indexer) saveStats(ctx context.Context) error {
	ps := persistedStats{
		RecordCount: idx.stats.RecordCount,
		LastRebuild: idx.stats.LastRebuild,
		LastSync:    idx.stats.LastSync,
	}
	data, err := json.Marshal(ps)
	if err != nil {
		return err
	}

	return idx.storage.Put(ctx, EntryPrefixStats+StatsVersion, data)
}

func (idx *Indexer) loadStats(ctx context.Context) error {
	data, err := idx.storage.Get(ctx, EntryPrefixStats+StatsVersion)
	if err != nil {
		if errors.Is(err, sriracha.ErrNotFound) {
			return nil
		}
		return err
	}

	var ps persistedStats
	if err := json.Unmarshal(data, &ps); err != nil {
		return err
	}

	idx.stats.RecordCount = ps.RecordCount
	idx.stats.LastRebuild = ps.LastRebuild
	idx.stats.LastSync = ps.LastSync
	return nil
}
