package indexer

import (
	"context"
	"sort"
	"strings"
	"sync"

	"go.sriracha.dev/sriracha"
)

const memCheckpointKey = "__checkpoint__"

// MemoryStorage is a zero-dependency in-memory implementation of sriracha.IndexStorage.
// It is intended for tests and environments that do not require persistence.
// It does not implement io.Closer or StorageSizer.
type MemoryStorage struct {
	data sync.Map // values are []byte (key/value entries) or string (checkpoint)
}

// NewMemoryStorage returns a new empty MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{}
}

func (s *MemoryStorage) Put(ctx context.Context, key string, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	cp := make([]byte, len(value))
	copy(cp, value)
	s.data.Store(key, cp)
	return nil
}

func (s *MemoryStorage) Get(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v, ok := s.data.Load(key)
	if !ok {
		return nil, sriracha.ErrNotFound
	}
	src, ok := v.([]byte)
	if !ok {
		return nil, sriracha.ErrNotFound
	}
	cp := make([]byte, len(src))
	copy(cp, src)
	return cp, nil
}

func (s *MemoryStorage) Scan(ctx context.Context, prefix string, fn func(key string, value []byte) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	type kv struct {
		key string
		val []byte
	}
	var pairs []kv
	s.data.Range(func(k, v any) bool {
		ks, ok := k.(string)
		if !ok {
			return true
		}
		if !strings.HasPrefix(ks, prefix) {
			return true
		}
		src, ok := v.([]byte)
		if !ok {
			return true
		}
		cp := make([]byte, len(src))
		copy(cp, src)
		pairs = append(pairs, kv{ks, cp})
		return true
	})
	sort.Slice(pairs, func(i, j int) bool { return pairs[i].key < pairs[j].key })
	for _, p := range pairs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(p.key, p.val); err != nil {
			return err
		}
	}
	return nil
}

func (s *MemoryStorage) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.data.Delete(key)
	return nil
}

func (s *MemoryStorage) SaveCheckpoint(ctx context.Context, token string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.data.Store(memCheckpointKey, token)
	return nil
}

func (s *MemoryStorage) LoadCheckpoint(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	v, ok := s.data.Load(memCheckpointKey)
	if !ok {
		return "", nil
	}
	tok, ok := v.(string)
	if !ok {
		return "", nil
	}
	return tok, nil
}
