package indexer

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bits-and-blooms/bitset"
	mocksriracha "go.sriracha.dev/mock/sriracha"
	"go.sriracha.dev/sriracha"
)

// testFS returns a minimal 2-field FieldSet for tests.
func testFS() sriracha.FieldSet {
	return sriracha.FieldSet{
		Version: "test-v1",
		Fields: []sriracha.FieldSpec{
			{Path: sriracha.FieldNameGiven, Required: false, Weight: 1.0},
			{Path: sriracha.FieldNameFamily, Required: false, Weight: 1.0},
		},
		BloomParams: sriracha.DefaultBloomConfig(),
	}
}

func testSecret() []byte {
	return []byte("test-secret-32-bytes-long!!!!!!")
}

func newTestIndexer(t *testing.T) *Indexer {
	t.Helper()
	idx, err := New(NewMemoryStorage(), testFS(), testSecret())
	require.NoError(t, err)
	t.Cleanup(func() { _ = idx.Close() })
	return idx
}

// scanSource returns a MockRecordSource whose Scan iterates records.
func scanSource(t *testing.T, records map[string]sriracha.RawRecord) *mocksriracha.MockRecordSource {
	t.Helper()
	src := mocksriracha.NewMockRecordSource(t)
	src.EXPECT().Scan(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, fn func(string, sriracha.RawRecord) error) error {
			for id, r := range records {
				if err := fn(id, r); err != nil {
					return err
				}
			}
			return nil
		})
	return src
}

// syncSource returns a MockIncrementalRecordSource whose ScanSince iterates delta.
func syncSource(t *testing.T, delta map[string]sriracha.RawRecord) *mocksriracha.MockIncrementalRecordSource {
	t.Helper()
	src := mocksriracha.NewMockIncrementalRecordSource(t)
	src.EXPECT().ScanSince(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ string, fn func(string, sriracha.RawRecord) error) error {
			for id, r := range delta {
				if err := fn(id, r); err != nil {
					return err
				}
			}
			return nil
		})
	return src
}

func TestNew(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		makeStorage func(*testing.T) sriracha.IndexStorage
		fs          sriracha.FieldSet
		secret      []byte
		wantErr     bool
	}{
		{
			name:        "nil storage",
			makeStorage: func(*testing.T) sriracha.IndexStorage { return nil },
			fs:          testFS(),
			secret:      testSecret(),
			wantErr:     true,
		},
		{
			name:        "empty secret",
			makeStorage: func(*testing.T) sriracha.IndexStorage { return NewMemoryStorage() },
			fs:          testFS(),
			secret:      []byte{},
			wantErr:     true,
		},
		{
			name:        "invalid fieldset",
			makeStorage: func(*testing.T) sriracha.IndexStorage { return NewMemoryStorage() },
			fs:          func() sriracha.FieldSet { fs := testFS(); fs.Version = ""; return fs }(),
			secret:      testSecret(),
			wantErr:     true,
		},
		{
			name:        "valid",
			makeStorage: func(*testing.T) sriracha.IndexStorage { return NewMemoryStorage() },
			fs:          testFS(),
			secret:      testSecret(),
			wantErr:     false,
		},
		{
			name: "malformed stats",
			makeStorage: func(t *testing.T) sriracha.IndexStorage {
				t.Helper()
				m := NewMemoryStorage()
				require.NoError(t, m.Put(context.Background(), "stats:v1", []byte("not-json")))
				return m
			},
			fs:      testFS(),
			secret:  testSecret(),
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			idx, err := New(tc.makeStorage(t), tc.fs, tc.secret)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				t.Cleanup(func() { _ = idx.Close() })
				assert.NotNil(t, idx)
			}
		})
	}

	t.Run("loads existing stats", func(t *testing.T) {
		t.Parallel()
		mem := NewMemoryStorage()
		idx, err := New(mem, testFS(), testSecret())
		require.NoError(t, err)
		t.Cleanup(func() { _ = idx.Close() })
		require.NoError(t, idx.Rebuild(context.Background(), scanSource(t, map[string]sriracha.RawRecord{
			"r1": {sriracha.FieldNameGiven: "Alice", sriracha.FieldNameFamily: "Smith"},
		})))
		idx2, err := New(mem, testFS(), testSecret())
		require.NoError(t, err)
		t.Cleanup(func() { _ = idx2.Close() })
		assert.Equal(t, int64(1), idx2.Stats().RecordCount)
	})

	t.Run("stats get error", func(t *testing.T) {
		t.Parallel()
		sentinel := errors.New("get fail")
		storage := mocksriracha.NewMockIndexStorage(t)
		storage.EXPECT().Get(mock.Anything, "stats:v1").Return(nil, sentinel)
		_, err := New(storage, testFS(), testSecret())
		require.ErrorIs(t, err, sentinel)
	})
}

func TestNewDefault(t *testing.T) {
	t.Parallel()

	t.Run("valid", func(t *testing.T) {
		t.Parallel()
		idx, err := NewDefault(t.TempDir(), testFS(), testSecret())
		require.NoError(t, err)
		require.NotNil(t, idx)
		require.NoError(t, idx.Close())
	})

	t.Run("open badger error", func(t *testing.T) {
		t.Parallel()
		f, err := os.CreateTemp(t.TempDir(), "notadir")
		require.NoError(t, err)
		_ = f.Close()
		_, err = NewDefault(f.Name(), testFS(), testSecret())
		require.Error(t, err)
	})

	t.Run("invalid fieldset closes storage", func(t *testing.T) {
		t.Parallel()
		fs := testFS()
		fs.Version = ""
		_, err := NewDefault(t.TempDir(), fs, testSecret())
		require.Error(t, err)
	})
}

// runStorageContract verifies the common IndexStorage contract for any implementation.
func runStorageContract(t *testing.T, makeStorage func(*testing.T) sriracha.IndexStorage) {
	t.Helper()
	ctx := context.Background()

	t.Run("put and get", func(t *testing.T) {
		t.Parallel()
		s := makeStorage(t)
		require.NoError(t, s.Put(ctx, "k", []byte("v")))
		val, err := s.Get(ctx, "k")
		require.NoError(t, err)
		assert.Equal(t, []byte("v"), val)
	})

	t.Run("get missing key returns ErrNotFound", func(t *testing.T) {
		t.Parallel()
		s := makeStorage(t)
		val, err := s.Get(ctx, "absent")
		require.ErrorIs(t, err, sriracha.ErrNotFound)
		assert.Nil(t, val)
	})

	t.Run("delete existing", func(t *testing.T) {
		t.Parallel()
		s := makeStorage(t)
		require.NoError(t, s.Put(ctx, "k", []byte("v")))
		require.NoError(t, s.Delete(ctx, "k"))
		val, err := s.Get(ctx, "k")
		require.ErrorIs(t, err, sriracha.ErrNotFound)
		assert.Nil(t, val)
	})

	t.Run("delete missing is no-op", func(t *testing.T) {
		t.Parallel()
		s := makeStorage(t)
		require.NoError(t, s.Delete(ctx, "absent"))
	})

	t.Run("scan prefix", func(t *testing.T) {
		t.Parallel()
		s := makeStorage(t)
		require.NoError(t, s.Put(ctx, "foo:1", []byte("a")))
		require.NoError(t, s.Put(ctx, "foo:2", []byte("b")))
		require.NoError(t, s.Put(ctx, "bar:1", []byte("c")))
		var keys []string
		require.NoError(t, s.Scan(ctx, "foo:", func(k string, _ []byte) error {
			keys = append(keys, k)
			return nil
		}))
		assert.ElementsMatch(t, []string{"foo:1", "foo:2"}, keys)
	})

	t.Run("scan order", func(t *testing.T) {
		t.Parallel()
		s := makeStorage(t)
		require.NoError(t, s.Put(ctx, "p:z", []byte("z")))
		require.NoError(t, s.Put(ctx, "p:a", []byte("a")))
		require.NoError(t, s.Put(ctx, "p:m", []byte("m")))
		var keys []string
		require.NoError(t, s.Scan(ctx, "p:", func(k string, _ []byte) error {
			keys = append(keys, k)
			return nil
		}))
		assert.Equal(t, []string{"p:a", "p:m", "p:z"}, keys)
	})

	t.Run("checkpoint roundtrip", func(t *testing.T) {
		t.Parallel()
		s := makeStorage(t)
		require.NoError(t, s.SaveCheckpoint(ctx, "tok123"))
		cp, err := s.LoadCheckpoint(ctx)
		require.NoError(t, err)
		assert.Equal(t, "tok123", cp)
	})

	t.Run("empty checkpoint", func(t *testing.T) {
		t.Parallel()
		s := makeStorage(t)
		cp, err := s.LoadCheckpoint(ctx)
		require.NoError(t, err)
		assert.Equal(t, "", cp)
	})

	t.Run("context canceled", func(t *testing.T) {
		t.Parallel()
		s := makeStorage(t)
		cctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.Error(t, s.Put(cctx, "k", []byte("v")))
		_, err := s.Get(cctx, "k")
		require.Error(t, err)
		require.Error(t, s.Scan(cctx, "k", func(string, []byte) error { return nil }))
		require.Error(t, s.Delete(cctx, "k"))
		require.Error(t, s.SaveCheckpoint(cctx, "tok"))
		_, err = s.LoadCheckpoint(cctx)
		require.Error(t, err)
	})

	t.Run("scan context canceled mid-iteration", func(t *testing.T) {
		t.Parallel()
		s := makeStorage(t)
		bg := context.Background()
		require.NoError(t, s.Put(bg, "p:1", []byte("a")))
		require.NoError(t, s.Put(bg, "p:2", []byte("b")))

		cctx, cancel := context.WithCancel(bg)
		defer cancel()
		count := 0
		err := s.Scan(cctx, "p:", func(_ string, _ []byte) error {
			count++
			if count == 1 {
				cancel()
			}
			return nil
		})
		require.Error(t, err)
	})
}

func TestMemoryStorage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	runStorageContract(t, func(t *testing.T) sriracha.IndexStorage {
		return NewMemoryStorage()
	})

	t.Run("scan error propagation", func(t *testing.T) {
		t.Parallel()
		s := NewMemoryStorage()
		require.NoError(t, s.Put(ctx, "p:1", []byte("v")))
		sentinel := errors.New("stop")
		err := s.Scan(ctx, "p:", func(_ string, _ []byte) error { return sentinel })
		assert.ErrorIs(t, err, sentinel)
	})

	t.Run("scan skips checkpoint key", func(t *testing.T) {
		t.Parallel()
		s := NewMemoryStorage()
		bg := context.Background()
		require.NoError(t, s.Put(bg, "k:1", []byte("a")))
		require.NoError(t, s.SaveCheckpoint(bg, "tok"))
		var keys []string
		require.NoError(t, s.Scan(bg, "", func(k string, _ []byte) error {
			keys = append(keys, k)
			return nil
		}))
		assert.Equal(t, []string{"k:1"}, keys)
	})

	t.Run("get checkpoint key returns not found", func(t *testing.T) {
		t.Parallel()
		s := NewMemoryStorage()
		// SaveCheckpoint stores a string under memCheckpointKey; Get expects []byte,
		// so the type assertion fails and ErrNotFound is returned.
		require.NoError(t, s.SaveCheckpoint(ctx, "tok"))
		_, err := s.Get(ctx, memCheckpointKey)
		require.ErrorIs(t, err, sriracha.ErrNotFound)
	})

	t.Run("load checkpoint after byte put returns empty", func(t *testing.T) {
		t.Parallel()
		s := NewMemoryStorage()
		// Put stores []byte under memCheckpointKey; LoadCheckpoint expects a string,
		// so the type assertion fails and the empty string is returned.
		require.NoError(t, s.Put(ctx, memCheckpointKey, []byte("x")))
		cp, err := s.LoadCheckpoint(ctx)
		require.NoError(t, err)
		assert.Equal(t, "", cp)
	})

	t.Run("scan skips non-string key", func(t *testing.T) {
		t.Parallel()
		s := NewMemoryStorage()
		s.data.Store(42, []byte("v")) // integer key, not string
		var keys []string
		require.NoError(t, s.Scan(ctx, "", func(k string, _ []byte) error {
			keys = append(keys, k)
			return nil
		}))
		assert.Empty(t, keys)
	})
}

func TestBadgerStorage(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	newBadger := func(t *testing.T) *BadgerStorage {
		t.Helper()
		s, err := OpenBadgerInMemory()
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })
		return s
	}

	runStorageContract(t, func(t *testing.T) sriracha.IndexStorage {
		return newBadger(t)
	})

	t.Run("size bytes non-negative", func(t *testing.T) {
		t.Parallel()
		s := newBadger(t)
		require.NoError(t, s.Put(ctx, "k", []byte("v")))
		sz, err := s.SizeBytes(ctx)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, sz, int64(0))
	})

	t.Run("close", func(t *testing.T) {
		t.Parallel()
		s, err := OpenBadgerInMemory()
		require.NoError(t, err)
		require.NoError(t, s.Close())
	})

	t.Run("put batch and get each", func(t *testing.T) {
		t.Parallel()
		s := newBadger(t)
		require.NoError(t, s.PutBatch(ctx, map[string][]byte{
			"k1": []byte("v1"),
			"k2": []byte("v2"),
		}))
		v1, err := s.Get(ctx, "k1")
		require.NoError(t, err)
		assert.Equal(t, []byte("v1"), v1)
		v2, err := s.Get(ctx, "k2")
		require.NoError(t, err)
		assert.Equal(t, []byte("v2"), v2)
	})

	t.Run("delete batch removes all keys", func(t *testing.T) {
		t.Parallel()
		s := newBadger(t)
		require.NoError(t, s.PutBatch(ctx, map[string][]byte{
			"k1": []byte("v1"),
			"k2": []byte("v2"),
		}))
		require.NoError(t, s.DeleteBatch(ctx, []string{"k1", "k2"}))
		_, err := s.Get(ctx, "k1")
		require.ErrorIs(t, err, sriracha.ErrNotFound)
		_, err = s.Get(ctx, "k2")
		require.ErrorIs(t, err, sriracha.ErrNotFound)
	})

	t.Run("put batch context canceled", func(t *testing.T) {
		t.Parallel()
		s := newBadger(t)
		cctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.Error(t, s.PutBatch(cctx, map[string][]byte{"k": []byte("v")}))
	})

	t.Run("delete batch context canceled", func(t *testing.T) {
		t.Parallel()
		s := newBadger(t)
		cctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.Error(t, s.DeleteBatch(cctx, []string{"k"}))
	})

	t.Run("get with empty key returns error", func(t *testing.T) {
		t.Parallel()
		s := newBadger(t)
		// Badger returns ErrEmptyKey for txn.Get(""), which is not ErrKeyNotFound,
		// so it falls through to the non-ErrKeyNotFound error branch.
		_, err := s.Get(ctx, "")
		require.Error(t, err)
	})

	t.Run("put batch with empty key returns error", func(t *testing.T) {
		t.Parallel()
		s := newBadger(t)
		// Badger returns ErrEmptyKey for txn.Set("", ...) inside PutBatch.
		err := s.PutBatch(ctx, map[string][]byte{"": []byte("v")})
		require.Error(t, err)
	})

	t.Run("delete batch with empty key returns error", func(t *testing.T) {
		t.Parallel()
		s := newBadger(t)
		// Badger returns ErrEmptyKey for txn.Delete("") inside DeleteBatch.
		err := s.DeleteBatch(ctx, []string{""})
		require.Error(t, err)
	})

	t.Run("scan fn error propagation", func(t *testing.T) {
		t.Parallel()
		s := newBadger(t)
		require.NoError(t, s.Put(ctx, "p:1", []byte("v")))
		sentinel := errors.New("fn error")
		err := s.Scan(ctx, "p:", func(_ string, _ []byte) error { return sentinel })
		require.ErrorIs(t, err, sentinel)
	})

	t.Run("scan value copy error", func(t *testing.T) {
		t.Parallel()
		s := newBadger(t)
		require.NoError(t, s.Put(ctx, "k:1", []byte("v")))
		sentinel := errors.New("vcopy fail")
		s.valueCopyFn = func(*badger.Item) ([]byte, error) { return nil, sentinel }
		err := s.Scan(ctx, "k:", func(_ string, _ []byte) error { return nil })
		require.ErrorIs(t, err, sentinel)
	})

	t.Run("load checkpoint non-ErrKeyNotFound error", func(t *testing.T) {
		t.Parallel()
		s := newBadger(t)
		// Empty key triggers ErrEmptyKey from Badger, which is not ErrKeyNotFound,
		// so it falls through to the non-ErrKeyNotFound error branch.
		s.checkpointKey = ""
		_, err := s.LoadCheckpoint(ctx)
		require.Error(t, err)
	})

	t.Run("load checkpoint value copy error", func(t *testing.T) {
		t.Parallel()
		s := newBadger(t)
		require.NoError(t, s.SaveCheckpoint(ctx, "tok"))
		sentinel := errors.New("vcopy fail")
		s.valueCopyFn = func(*badger.Item) ([]byte, error) { return nil, sentinel }
		_, err := s.LoadCheckpoint(ctx)
		require.ErrorIs(t, err, sentinel)
	})
}

func TestRebuild(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	cases := []struct {
		name  string
		setup func(t *testing.T, idx *Indexer, sentinel error) sriracha.RecordSource
	}{
		{
			name: "source error",
			setup: func(t *testing.T, _ *Indexer, sentinel error) sriracha.RecordSource {
				t.Helper()
				src := mocksriracha.NewMockRecordSource(t)
				src.EXPECT().Scan(mock.Anything, mock.Anything).Return(sentinel)
				return src
			},
		},
		{
			name: "storage scan error",
			setup: func(t *testing.T, idx *Indexer, sentinel error) sriracha.RecordSource {
				t.Helper()
				storage := mocksriracha.NewMockIndexStorage(t)
				storage.EXPECT().Scan(mock.Anything, "det:", mock.Anything).Return(sentinel)
				idx.storage = storage
				return mocksriracha.NewMockRecordSource(t)
			},
		},
		{
			name: "storage delete error",
			setup: func(t *testing.T, idx *Indexer, sentinel error) sriracha.RecordSource {
				t.Helper()
				storage := mocksriracha.NewMockIndexStorage(t)
				storage.EXPECT().Scan(mock.Anything, "det:", mock.Anything).RunAndReturn(
					func(_ context.Context, _ string, fn func(string, []byte) error) error {
						return fn("det:somekey", nil)
					})
				storage.EXPECT().Scan(mock.Anything, "prob:", mock.Anything).Return(nil)
				storage.EXPECT().Scan(mock.Anything, "meta:", mock.Anything).Return(nil)
				storage.EXPECT().Delete(mock.Anything, "det:somekey").Return(sentinel)
				idx.storage = storage
				return mocksriracha.NewMockRecordSource(t)
			},
		},
		{
			name: "save stats error",
			setup: func(t *testing.T, idx *Indexer, sentinel error) sriracha.RecordSource {
				t.Helper()
				storage := mocksriracha.NewMockIndexStorage(t)
				storage.EXPECT().Scan(mock.Anything, "det:", mock.Anything).Return(nil)
				storage.EXPECT().Scan(mock.Anything, "prob:", mock.Anything).Return(nil)
				storage.EXPECT().Scan(mock.Anything, "meta:", mock.Anything).Return(nil)
				storage.EXPECT().Put(mock.Anything, "stats:v1", mock.Anything).Return(sentinel)
				idx.storage = storage
				src := mocksriracha.NewMockRecordSource(t)
				src.EXPECT().Scan(mock.Anything, mock.Anything).Return(nil)
				return src
			},
		},
		{
			name: "index record det put error",
			setup: func(t *testing.T, idx *Indexer, sentinel error) sriracha.RecordSource {
				t.Helper()
				storage := mocksriracha.NewMockIndexStorage(t)
				storage.EXPECT().Scan(mock.Anything, "det:", mock.Anything).Return(nil)
				storage.EXPECT().Scan(mock.Anything, "prob:", mock.Anything).Return(nil)
				storage.EXPECT().Scan(mock.Anything, "meta:", mock.Anything).Return(nil)
				// Sequential: det Put fails → prob and meta Puts are not attempted.
				storage.EXPECT().Put(mock.Anything, mock.MatchedBy(func(k string) bool {
					return strings.HasPrefix(k, "det:")
				}), mock.Anything).Return(sentinel)
				idx.storage = storage
				src := mocksriracha.NewMockRecordSource(t)
				src.EXPECT().Scan(mock.Anything, mock.Anything).RunAndReturn(
					func(_ context.Context, fn func(string, sriracha.RawRecord) error) error {
						return fn("r1", sriracha.RawRecord{sriracha.FieldNameGiven: "Alice", sriracha.FieldNameFamily: "Smith"})
					})
				return src
			},
		},
		{
			name: "index record prob put error",
			setup: func(t *testing.T, idx *Indexer, sentinel error) sriracha.RecordSource {
				t.Helper()
				storage := mocksriracha.NewMockIndexStorage(t)
				storage.EXPECT().Scan(mock.Anything, "det:", mock.Anything).Return(nil)
				storage.EXPECT().Scan(mock.Anything, "prob:", mock.Anything).Return(nil)
				storage.EXPECT().Scan(mock.Anything, "meta:", mock.Anything).Return(nil)
				// Sequential: det Put succeeds, prob Put fails → meta Put not attempted.
				storage.EXPECT().Put(mock.Anything, mock.MatchedBy(func(k string) bool {
					return strings.HasPrefix(k, "det:")
				}), mock.Anything).Return(nil)
				storage.EXPECT().Put(mock.Anything, mock.MatchedBy(func(k string) bool {
					return strings.HasPrefix(k, "prob:")
				}), mock.Anything).Return(sentinel)
				idx.storage = storage
				src := mocksriracha.NewMockRecordSource(t)
				src.EXPECT().Scan(mock.Anything, mock.Anything).RunAndReturn(
					func(_ context.Context, fn func(string, sriracha.RawRecord) error) error {
						return fn("r1", sriracha.RawRecord{sriracha.FieldNameGiven: "Alice", sriracha.FieldNameFamily: "Smith"})
					})
				return src
			},
		},
		{
			name: "transactor delete batch error",
			setup: func(t *testing.T, idx *Indexer, sentinel error) sriracha.RecordSource {
				t.Helper()
				mem := NewMemoryStorage()
				require.NoError(t, mem.Put(context.Background(), "det:foo", []byte("r1")))
				idx.storage = &transactorStorage{MemoryStorage: mem, deleteBatchErr: sentinel}
				// Rebuild fails at DeleteBatch before reaching src.Scan, so use a
				// plain sliceSource instead of a mock that would assert Scan is called.
				return &sliceSource{}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sentinel := errors.New("sentinel")
			idx := newTestIndexer(t)
			src := tc.setup(t, idx, sentinel)
			err := idx.Rebuild(ctx, src)
			require.ErrorIs(t, err, sentinel)
		})
	}

	t.Run("tokenize det error", func(t *testing.T) {
		t.Parallel()
		// A date field rejects non-ISO-8601 values; this triggers the TokenizeRecord
		// error path inside indexRecord.
		fs := sriracha.FieldSet{
			Version:     "date-v1",
			Fields:      []sriracha.FieldSpec{{Path: sriracha.FieldDateBirth, Required: false, Weight: 1.0}},
			BloomParams: sriracha.DefaultBloomConfig(),
		}
		idx, err := New(NewMemoryStorage(), fs, testSecret())
		require.NoError(t, err)
		t.Cleanup(func() { _ = idx.Close() })
		err = idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{
			"r1": {sriracha.FieldDateBirth: "not-a-date"},
		}))
		require.Error(t, err)
	})

	t.Run("empty source", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		err := idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{}))
		require.NoError(t, err)
		assert.Equal(t, int64(0), idx.Stats().RecordCount)
	})

	t.Run("two records", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{
			"r1": {sriracha.FieldNameGiven: "Alice", sriracha.FieldNameFamily: "Smith"},
			"r2": {sriracha.FieldNameGiven: "Bob", sriracha.FieldNameFamily: "Jones"},
		})))
		assert.Equal(t, int64(2), idx.Stats().RecordCount)
		assert.False(t, idx.Stats().LastRebuild.IsZero())
	})

	t.Run("rebuild twice clears prior", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		records := map[string]sriracha.RawRecord{
			"r1": {sriracha.FieldNameGiven: "Alice", sriracha.FieldNameFamily: "Smith"},
			"r2": {sriracha.FieldNameGiven: "Bob", sriracha.FieldNameFamily: "Jones"},
		}
		src := mocksriracha.NewMockRecordSource(t)
		src.EXPECT().Scan(mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, fn func(string, sriracha.RawRecord) error) error {
				for id, r := range records {
					if err := fn(id, r); err != nil {
						return err
					}
				}
				return nil
			}).Times(2)
		require.NoError(t, idx.Rebuild(ctx, src))
		require.NoError(t, idx.Rebuild(ctx, src))
		assert.Equal(t, int64(2), idx.Stats().RecordCount)
	})

	t.Run("rebuild twice with badger uses transactor", func(t *testing.T) {
		t.Parallel()
		s, err := OpenBadgerInMemory()
		require.NoError(t, err)
		idx, err := New(s, testFS(), testSecret())
		require.NoError(t, err)
		defer func() { _ = idx.Close() }()

		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{
			"r1": {sriracha.FieldNameGiven: "Alice", sriracha.FieldNameFamily: "Smith"},
		})))
		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{})))
		assert.Equal(t, int64(0), idx.Stats().RecordCount)
	})
}

func TestSync(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	cases := []struct {
		name  string
		setup func(t *testing.T, idx *Indexer, sentinel error) sriracha.IncrementalRecordSource
	}{
		{
			name: "storage error",
			setup: func(t *testing.T, _ *Indexer, sentinel error) sriracha.IncrementalRecordSource {
				t.Helper()
				src := mocksriracha.NewMockIncrementalRecordSource(t)
				src.EXPECT().ScanSince(mock.Anything, mock.Anything, mock.Anything).Return(sentinel)
				return src
			},
		},
		{
			name: "load checkpoint error",
			setup: func(t *testing.T, idx *Indexer, sentinel error) sriracha.IncrementalRecordSource {
				t.Helper()
				storage := mocksriracha.NewMockIndexStorage(t)
				storage.EXPECT().LoadCheckpoint(mock.Anything).Return("", sentinel)
				idx.storage = storage
				return mocksriracha.NewMockIncrementalRecordSource(t)
			},
		},
		{
			name: "save checkpoint error",
			setup: func(t *testing.T, idx *Indexer, sentinel error) sriracha.IncrementalRecordSource {
				t.Helper()
				storage := mocksriracha.NewMockIndexStorage(t)
				storage.EXPECT().LoadCheckpoint(mock.Anything).Return("", nil)
				storage.EXPECT().SaveCheckpoint(mock.Anything, mock.Anything).Return(sentinel)
				idx.storage = storage
				src := mocksriracha.NewMockIncrementalRecordSource(t)
				src.EXPECT().ScanSince(mock.Anything, mock.Anything, mock.Anything).Return(nil)
				return src
			},
		},
		{
			name: "index record put error during sync",
			setup: func(t *testing.T, idx *Indexer, sentinel error) sriracha.IncrementalRecordSource {
				t.Helper()
				storage := mocksriracha.NewMockIndexStorage(t)
				storage.EXPECT().LoadCheckpoint(mock.Anything).Return("", nil)
				storage.EXPECT().Get(mock.Anything, "meta:r1").Return(nil, sriracha.ErrNotFound)
				storage.EXPECT().Put(mock.Anything, mock.Anything, mock.Anything).Return(sentinel)
				idx.storage = storage
				src := mocksriracha.NewMockIncrementalRecordSource(t)
				src.EXPECT().ScanSince(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
					func(_ context.Context, _ string, fn func(string, sriracha.RawRecord) error) error {
						return fn("r1", sriracha.RawRecord{sriracha.FieldNameGiven: "Alice", sriracha.FieldNameFamily: "Smith"})
					})
				return src
			},
		},
		{
			name: "save stats error",
			setup: func(t *testing.T, idx *Indexer, sentinel error) sriracha.IncrementalRecordSource {
				t.Helper()
				storage := mocksriracha.NewMockIndexStorage(t)
				storage.EXPECT().LoadCheckpoint(mock.Anything).Return("", nil)
				storage.EXPECT().SaveCheckpoint(mock.Anything, mock.Anything).Return(nil)
				storage.EXPECT().Put(mock.Anything, "stats:v1", mock.Anything).Return(sentinel)
				idx.storage = storage
				src := mocksriracha.NewMockIncrementalRecordSource(t)
				src.EXPECT().ScanSince(mock.Anything, mock.Anything, mock.Anything).Return(nil)
				return src
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sentinel := errors.New("sentinel")
			idx := newTestIndexer(t)
			src := tc.setup(t, idx, sentinel)
			err := idx.Sync(ctx, src)
			require.ErrorIs(t, err, sentinel)
		})
	}

	t.Run("new records", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		require.NoError(t, idx.Sync(ctx, syncSource(t, map[string]sriracha.RawRecord{
			"r1": {sriracha.FieldNameGiven: "Alice", sriracha.FieldNameFamily: "Smith"},
		})))
		assert.Equal(t, int64(1), idx.Stats().RecordCount)
		assert.False(t, idx.Stats().LastSync.IsZero())
	})

	t.Run("deletion via nil record", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{
			"r1": {sriracha.FieldNameGiven: "Alice", sriracha.FieldNameFamily: "Smith"},
		})))
		assert.Equal(t, int64(1), idx.Stats().RecordCount)

		src := mocksriracha.NewMockIncrementalRecordSource(t)
		src.EXPECT().ScanSince(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, _ string, fn func(string, sriracha.RawRecord) error) error {
				return fn("r1", nil)
			})
		require.NoError(t, idx.Sync(ctx, src))
		assert.Equal(t, int64(0), idx.Stats().RecordCount)
	})

	t.Run("deletion via empty record", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{
			"r1": {sriracha.FieldNameGiven: "Alice", sriracha.FieldNameFamily: "Smith"},
		})))

		src := mocksriracha.NewMockIncrementalRecordSource(t)
		src.EXPECT().ScanSince(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, _ string, fn func(string, sriracha.RawRecord) error) error {
				return fn("r1", sriracha.RawRecord{})
			})
		require.NoError(t, idx.Sync(ctx, src))
		assert.Equal(t, int64(0), idx.Stats().RecordCount)
	})

	t.Run("checkpoint saved after sync", func(t *testing.T) {
		t.Parallel()
		mem := NewMemoryStorage()
		idx, err := New(mem, testFS(), testSecret())
		require.NoError(t, err)
		t.Cleanup(func() { _ = idx.Close() })

		before := time.Now()
		require.NoError(t, idx.Sync(ctx, syncSource(t, map[string]sriracha.RawRecord{})))
		after := time.Now()

		cp, err := mem.LoadCheckpoint(ctx)
		require.NoError(t, err)
		require.NotEmpty(t, cp)

		parsed, err := time.Parse(time.RFC3339, cp)
		require.NoError(t, err)
		assert.True(t, !parsed.Before(before.Truncate(time.Second)))
		assert.True(t, !parsed.After(after.Add(time.Second)))
	})

	t.Run("checkpoint passed to ScanSince", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)

		var cp1 string
		src1 := mocksriracha.NewMockIncrementalRecordSource(t)
		src1.EXPECT().ScanSince(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, cp string, _ func(string, sriracha.RawRecord) error) error {
				cp1 = cp
				return nil
			})
		require.NoError(t, idx.Sync(ctx, src1))
		assert.Equal(t, "", cp1)

		var cp2 string
		src2 := mocksriracha.NewMockIncrementalRecordSource(t)
		src2.EXPECT().ScanSince(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, cp string, _ func(string, sriracha.RawRecord) error) error {
				cp2 = cp
				return nil
			})
		require.NoError(t, idx.Sync(ctx, src2))
		assert.NotEmpty(t, cp2)
	})

	t.Run("delete error during sync nil record", func(t *testing.T) {
		t.Parallel()
		mem := NewMemoryStorage()
		idx, err := New(mem, testFS(), testSecret())
		require.NoError(t, err)
		t.Cleanup(func() { _ = idx.Close() })
		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{
			"r1": {sriracha.FieldNameGiven: "Alice", sriracha.FieldNameFamily: "Smith"},
		})))

		metaBytes, getErr := mem.Get(ctx, "meta:r1")
		require.NoError(t, getErr)

		sentinel := errors.New("delete fail")
		storage := mocksriracha.NewMockIndexStorage(t)
		storage.EXPECT().LoadCheckpoint(mock.Anything).Return("", nil)
		storage.EXPECT().Get(mock.Anything, "meta:r1").Return(metaBytes, nil)
		storage.EXPECT().Delete(mock.Anything, mock.Anything).Return(sentinel)
		idx.storage = storage

		src := mocksriracha.NewMockIncrementalRecordSource(t)
		src.EXPECT().ScanSince(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, _ string, fn func(string, sriracha.RawRecord) error) error {
				return fn("r1", nil)
			})
		err = idx.Sync(ctx, src)
		require.Error(t, err)
	})

	t.Run("delete error during sync non-nil record", func(t *testing.T) {
		t.Parallel()
		mem := NewMemoryStorage()
		idx, err := New(mem, testFS(), testSecret())
		require.NoError(t, err)
		t.Cleanup(func() { _ = idx.Close() })
		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{
			"r1": {sriracha.FieldNameGiven: "Alice", sriracha.FieldNameFamily: "Smith"},
		})))

		metaBytes, getErr := mem.Get(ctx, "meta:r1")
		require.NoError(t, getErr)

		sentinel := errors.New("delete fail")
		storage := mocksriracha.NewMockIndexStorage(t)
		storage.EXPECT().LoadCheckpoint(mock.Anything).Return("", nil)
		storage.EXPECT().Get(mock.Anything, "meta:r1").Return(metaBytes, nil)
		// Sequential: det Delete fails → prob and meta deletes are not attempted.
		storage.EXPECT().Delete(mock.Anything, mock.MatchedBy(func(k string) bool {
			return strings.HasPrefix(k, "det:")
		})).Return(sentinel)
		idx.storage = storage

		src := mocksriracha.NewMockIncrementalRecordSource(t)
		src.EXPECT().ScanSince(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, _ string, fn func(string, sriracha.RawRecord) error) error {
				return fn("r1", sriracha.RawRecord{sriracha.FieldNameGiven: "Alison", sriracha.FieldNameFamily: "Smith"})
			})
		err = idx.Sync(ctx, src)
		require.ErrorIs(t, err, sentinel)
	})

	t.Run("update preserves record count", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{
			"r1": {sriracha.FieldNameGiven: "Alice", sriracha.FieldNameFamily: "Smith"},
		})))
		assert.Equal(t, int64(1), idx.Stats().RecordCount)

		// Syncing an update to r1 must not change the count.
		require.NoError(t, idx.Sync(ctx, syncSource(t, map[string]sriracha.RawRecord{
			"r1": {sriracha.FieldNameGiven: "Alison", sriracha.FieldNameFamily: "Smith"},
		})))
		assert.Equal(t, int64(1), idx.Stats().RecordCount)
	})

	t.Run("delete non-existent does not decrement count", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{
			"r1": {sriracha.FieldNameGiven: "Alice", sriracha.FieldNameFamily: "Smith"},
		})))
		assert.Equal(t, int64(1), idx.Stats().RecordCount)

		// Syncing a deletion for a record not in the index must not decrement.
		require.NoError(t, idx.Sync(ctx, syncSource(t, map[string]sriracha.RawRecord{
			"ghost": nil,
		})))
		assert.Equal(t, int64(1), idx.Stats().RecordCount)
	})

	t.Run("sync delete with badger uses transactor", func(t *testing.T) {
		t.Parallel()
		s, err := OpenBadgerInMemory()
		require.NoError(t, err)
		idx, err := New(s, testFS(), testSecret())
		require.NoError(t, err)
		defer func() { _ = idx.Close() }()

		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{
			"r1": {sriracha.FieldNameGiven: "Alice", sriracha.FieldNameFamily: "Smith"},
		})))

		src := mocksriracha.NewMockIncrementalRecordSource(t)
		src.EXPECT().ScanSince(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
			func(_ context.Context, _ string, fn func(string, sriracha.RawRecord) error) error {
				return fn("r1", nil)
			})
		require.NoError(t, idx.Sync(ctx, src))
		assert.Equal(t, int64(0), idx.Stats().RecordCount)
	})
}

func TestMatch_Deterministic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("empty index", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		tr := sriracha.TokenRecord{
			FieldSetVersion: "test-v1",
			Mode:            sriracha.Deterministic,
			Payload:         []byte("somepayload"),
		}
		candidates, err := idx.Match(ctx, tr, sriracha.MatchConfig{})
		require.NoError(t, err)
		assert.Nil(t, candidates)
	})

	t.Run("exact match", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		rec := sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Alice",
			sriracha.FieldNameFamily: "Smith",
		}
		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{"r1": rec})))

		tr, err := idx.tok.TokenizeRecord(rec, idx.fs)
		require.NoError(t, err)

		candidates, err := idx.Match(ctx, tr, sriracha.MatchConfig{})
		require.NoError(t, err)
		require.Len(t, candidates, 1)
		assert.Equal(t, "r1", candidates[0].RecordID)
		assert.Equal(t, 1.0, candidates[0].Confidence)
	})

	t.Run("no cross-match", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		rec1 := sriracha.RawRecord{sriracha.FieldNameGiven: "Alice", sriracha.FieldNameFamily: "Smith"}
		rec2 := sriracha.RawRecord{sriracha.FieldNameGiven: "Bob", sriracha.FieldNameFamily: "Jones"}
		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{
			"r1": rec1, "r2": rec2,
		})))

		tr, err := idx.tok.TokenizeRecord(rec1, idx.fs)
		require.NoError(t, err)

		candidates, err := idx.Match(ctx, tr, sriracha.MatchConfig{})
		require.NoError(t, err)
		require.Len(t, candidates, 1)
		assert.Equal(t, "r1", candidates[0].RecordID)
	})

	t.Run("wrong version", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		tr := sriracha.TokenRecord{
			FieldSetVersion: "other-v1",
			Mode:            sriracha.Deterministic,
		}
		_, err := idx.Match(ctx, tr, sriracha.MatchConfig{})
		require.ErrorIs(t, err, sriracha.ErrFieldSetIncompatible("", ""))
	})

	t.Run("unknown mode", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		tr := sriracha.TokenRecord{
			FieldSetVersion: "test-v1",
			Mode:            sriracha.MatchMode(99),
		}
		_, err := idx.Match(ctx, tr, sriracha.MatchConfig{})
		require.ErrorIs(t, err, sriracha.ErrInternalError(""))
	})

	t.Run("storage get error", func(t *testing.T) {
		t.Parallel()
		sentinel := errors.New("get fail")
		idx := newTestIndexer(t)
		storage := mocksriracha.NewMockIndexStorage(t)
		storage.EXPECT().Get(mock.Anything, mock.Anything).Return(nil, sentinel)
		idx.storage = storage
		tr := sriracha.TokenRecord{
			FieldSetVersion: "test-v1",
			Mode:            sriracha.Deterministic,
			Payload:         []byte("somekey"),
		}
		_, err := idx.Match(ctx, tr, sriracha.MatchConfig{})
		require.ErrorIs(t, err, sentinel)
	})
}

func TestMatch_Probabilistic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("empty index", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		tr := sriracha.TokenRecord{
			FieldSetVersion: "test-v1",
			Mode:            sriracha.Probabilistic,
			Payload:         make([]byte, 2*fieldFilterBytes(sriracha.DefaultBloomConfig().SizeBits)),
		}
		candidates, err := idx.Match(ctx, tr, sriracha.MatchConfig{})
		require.NoError(t, err)
		assert.Empty(t, candidates)
	})

	t.Run("identical record scores near 1.0", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		rec := sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Alice",
			sriracha.FieldNameFamily: "Smith",
		}
		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{"r1": rec})))

		tr, err := idx.tok.TokenizeRecordBloom(rec, idx.fs)
		require.NoError(t, err)

		candidates, err := idx.Match(ctx, tr, sriracha.MatchConfig{Threshold: 0.9, MaxResults: 10})
		require.NoError(t, err)
		require.Len(t, candidates, 1)
		assert.Equal(t, "r1", candidates[0].RecordID)
		assert.InDelta(t, 1.0, candidates[0].Confidence, 0.01)
	})

	t.Run("dissimilar names below threshold", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		stored := sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Alice",
			sriracha.FieldNameFamily: "Smith",
		}
		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{"r1": stored})))

		query := sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Bob",
			sriracha.FieldNameFamily: "Jones",
		}
		tr, err := idx.tok.TokenizeRecordBloom(query, idx.fs)
		require.NoError(t, err)

		candidates, err := idx.Match(ctx, tr, sriracha.MatchConfig{Threshold: 0.85, MaxResults: 10})
		require.NoError(t, err)
		assert.Empty(t, candidates)
	})

	t.Run("similar typo above threshold", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		stored := sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Alice",
			sriracha.FieldNameFamily: "Smith",
		}
		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{"r1": stored})))

		query := sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Alise",
			sriracha.FieldNameFamily: "Smith",
		}
		tr, err := idx.tok.TokenizeRecordBloom(query, idx.fs)
		require.NoError(t, err)

		candidates, err := idx.Match(ctx, tr, sriracha.MatchConfig{Threshold: 0.5, MaxResults: 10})
		require.NoError(t, err)
		require.NotEmpty(t, candidates)
		assert.Equal(t, "r1", candidates[0].RecordID)
	})

	t.Run("wrong version", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		tr := sriracha.TokenRecord{
			FieldSetVersion: "other-v1",
			Mode:            sriracha.Probabilistic,
		}
		_, err := idx.Match(ctx, tr, sriracha.MatchConfig{})
		require.ErrorIs(t, err, sriracha.ErrFieldSetIncompatible("", ""))
	})

	t.Run("payload length mismatch", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		tr := sriracha.TokenRecord{
			FieldSetVersion: "test-v1",
			Mode:            sriracha.Probabilistic,
			Payload:         []byte("tooshort"),
		}
		_, err := idx.Match(ctx, tr, sriracha.MatchConfig{})
		require.ErrorIs(t, err, sriracha.ErrIndexCorrupted(""))
	})

	t.Run("max results limit", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		rec := sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Alice",
			sriracha.FieldNameFamily: "Smith",
		}
		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{
			"r1": rec,
			"r2": rec,
			"r3": rec,
		})))

		tr, err := idx.tok.TokenizeRecordBloom(rec, idx.fs)
		require.NoError(t, err)

		candidates, err := idx.Match(ctx, tr, sriracha.MatchConfig{Threshold: 0.5, MaxResults: 2})
		require.NoError(t, err)
		assert.Len(t, candidates, 2)
	})

	t.Run("results sorted by confidence", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		recExact := sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Alice",
			sriracha.FieldNameFamily: "Smith",
		}
		recTypo := sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Alise",
			sriracha.FieldNameFamily: "Smith",
		}
		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{
			"r-typo":  recTypo,
			"r-exact": recExact,
		})))

		tr, err := idx.tok.TokenizeRecordBloom(recExact, idx.fs)
		require.NoError(t, err)

		candidates, err := idx.Match(ctx, tr, sriracha.MatchConfig{Threshold: 0.5, MaxResults: 10})
		require.NoError(t, err)
		require.GreaterOrEqual(t, len(candidates), 2)
		for i := 1; i < len(candidates); i++ {
			assert.GreaterOrEqual(t, candidates[i-1].Confidence, candidates[i].Confidence)
		}
	})

	t.Run("custom threshold", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		rec := sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Alice",
			sriracha.FieldNameFamily: "Smith",
		}
		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{"r1": rec})))

		tr, err := idx.tok.TokenizeRecordBloom(rec, idx.fs)
		require.NoError(t, err)

		candidates, err := idx.Match(ctx, tr, sriracha.MatchConfig{Threshold: 1.01, MaxResults: 10})
		require.NoError(t, err)
		assert.Empty(t, candidates)
	})

	t.Run("custom field weight applied", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		rec := sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Alice",
			sriracha.FieldNameFamily: "Smith",
		}
		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{"r1": rec})))

		tr, err := idx.tok.TokenizeRecordBloom(rec, idx.fs)
		require.NoError(t, err)

		candidates, err := idx.Match(ctx, tr, sriracha.MatchConfig{
			Threshold:  0.9,
			MaxResults: 10,
			FieldWeights: []sriracha.FieldWeight{
				{Path: sriracha.FieldNameGiven, Weight: 2.0},
			},
		})
		require.NoError(t, err)
		require.Len(t, candidates, 1)
		assert.Equal(t, "r1", candidates[0].RecordID)
	})

	t.Run("storage scan error", func(t *testing.T) {
		t.Parallel()
		sentinel := errors.New("scan fail")
		idx := newTestIndexer(t)
		storage := mocksriracha.NewMockIndexStorage(t)
		storage.EXPECT().Scan(mock.Anything, mock.Anything, mock.Anything).Return(sentinel)
		idx.storage = storage
		tr := sriracha.TokenRecord{
			FieldSetVersion: "test-v1",
			Mode:            sriracha.Probabilistic,
			Payload:         make([]byte, 2*fieldFilterBytes(sriracha.DefaultBloomConfig().SizeBits)),
		}
		_, err := idx.Match(ctx, tr, sriracha.MatchConfig{})
		require.ErrorIs(t, err, sentinel)
	})

	t.Run("stored payload length mismatch", func(t *testing.T) {
		t.Parallel()
		mem := NewMemoryStorage()
		idx, err := New(mem, testFS(), testSecret())
		require.NoError(t, err)
		t.Cleanup(func() { _ = idx.Close() })
		require.NoError(t, mem.Put(ctx, "prob:test-v1:r1", []byte{0xFF}))

		tr := sriracha.TokenRecord{
			FieldSetVersion: "test-v1",
			Mode:            sriracha.Probabilistic,
			Payload:         make([]byte, 2*fieldFilterBytes(sriracha.DefaultBloomConfig().SizeBits)),
		}
		_, err = idx.Match(ctx, tr, sriracha.MatchConfig{Threshold: 0.01, MaxResults: 10})
		require.ErrorIs(t, err, sriracha.ErrIndexCorrupted(""))
	})
}

func TestScoreProbabilistic(t *testing.T) {
	t.Parallel()

	fs := testFS()
	fieldBytes := fieldFilterBytes(fs.BloomParams.SizeBits)
	totalBytes := len(fs.Fields) * fieldBytes

	// parseQueryBitsets converts a raw payload into the pre-parsed slice that
	// scoreProbabilistic now expects (mirrors the logic in matchProbabilistic).
	parseQueryBitsets := func(t *testing.T, payload []byte) []*bitset.BitSet {
		t.Helper()
		bss := make([]*bitset.BitSet, len(fs.Fields))
		for i := range fs.Fields {
			bss[i] = filterFromBytes(payload[i*fieldBytes : (i+1)*fieldBytes])
		}
		return bss
	}

	cases := []struct {
		name        string
		setupQuery  func() []byte
		setupStored func() []byte
		wantScore   float64
		wantErr     bool
	}{
		{
			name:        "identical payloads give 1.0",
			setupQuery:  func() []byte { p := make([]byte, totalBytes); p[0] = 0xFF; return p },
			setupStored: func() []byte { p := make([]byte, totalBytes); p[0] = 0xFF; return p },
			wantScore:   1.0,
		},
		{
			name:        "zero vs zero field is skipped",
			setupQuery:  func() []byte { p := make([]byte, totalBytes); p[fieldBytes] = 0xFF; return p },
			setupStored: func() []byte { p := make([]byte, totalBytes); p[fieldBytes] = 0xFF; return p },
			wantScore:   1.0,
		},
		{
			name:        "all fields absent gives 0.0",
			setupQuery:  func() []byte { return make([]byte, totalBytes) },
			setupStored: func() []byte { return make([]byte, totalBytes) },
			wantScore:   0.0,
		},
		{
			name:        "stored payload length mismatch error",
			setupQuery:  func() []byte { return make([]byte, totalBytes) },
			setupStored: func() []byte { return make([]byte, fieldBytes) }, // too short
			wantErr:     true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			query := tc.setupQuery()
			stored := tc.setupStored()
			queryBitsets := parseQueryBitsets(t, query)
			score, err := scoreProbabilistic(queryBitsets, stored, fs, sriracha.MatchConfig{}, fieldBytes)
			if tc.wantErr {
				require.ErrorIs(t, err, sriracha.ErrIndexCorrupted(""))
				return
			}
			require.NoError(t, err)
			assert.InDelta(t, tc.wantScore, score, 1e-9)
		})
	}
}

func TestFieldFilterBytes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		sizeBits uint32
		want     int
	}{
		{1024, 128},
		{64, 8},
		{65, 16},
	}
	for _, tc := range cases {
		t.Run("", func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, fieldFilterBytes(tc.sizeBits))
		})
	}
}

// benchRecords is a set of realistic PII records used for benchmarks.
var benchRecords = map[string]sriracha.RawRecord{
	"r1": {sriracha.FieldNameGiven: "Alice", sriracha.FieldNameFamily: "Smith"},
	"r2": {sriracha.FieldNameGiven: "Bob", sriracha.FieldNameFamily: "Jones"},
	"r3": {sriracha.FieldNameGiven: "Charlie", sriracha.FieldNameFamily: "Brown"},
	"r4": {sriracha.FieldNameGiven: "Diana", sriracha.FieldNameFamily: "Prince"},
	"r5": {sriracha.FieldNameGiven: "Edward", sriracha.FieldNameFamily: "Norton"},
}

func BenchmarkRebuild(b *testing.B) {
	ctx := context.Background()
	for range b.N {
		b.StopTimer()
		idx, _ := New(NewMemoryStorage(), testFS(), testSecret())
		src := &sliceSource{records: benchRecords}
		b.StartTimer()
		_ = idx.Rebuild(ctx, src)
	}
}

func BenchmarkMatchDeterministic(b *testing.B) {
	ctx := context.Background()
	idx, _ := New(NewMemoryStorage(), testFS(), testSecret())
	_ = idx.Rebuild(ctx, &sliceSource{records: benchRecords})
	rec := benchRecords["r1"]
	tr, _ := idx.tok.TokenizeRecord(rec, idx.fs)
	b.ResetTimer()
	for range b.N {
		_, _ = idx.Match(ctx, tr, sriracha.MatchConfig{})
	}
}

func BenchmarkMatchProbabilistic(b *testing.B) {
	ctx := context.Background()
	idx, _ := New(NewMemoryStorage(), testFS(), testSecret())
	_ = idx.Rebuild(ctx, &sliceSource{records: benchRecords})
	rec := benchRecords["r1"]
	tr, _ := idx.tok.TokenizeRecordBloom(rec, idx.fs)
	cfg := sriracha.MatchConfig{Threshold: 0.5, MaxResults: 10}
	b.ResetTimer()
	for range b.N {
		_, _ = idx.Match(ctx, tr, cfg)
	}
}

// transactorStorage wraps MemoryStorage and adds configurable Transactor behavior
// for testing code paths that require both IndexStorage and Transactor.
type transactorStorage struct {
	*MemoryStorage
	putBatchErr    error
	deleteBatchErr error
}

func (s *transactorStorage) PutBatch(_ context.Context, _ map[string][]byte) error {
	return s.putBatchErr
}

func (s *transactorStorage) DeleteBatch(_ context.Context, _ []string) error {
	return s.deleteBatchErr
}

// sliceSource is a minimal RecordSource backed by a static map.
type sliceSource struct {
	records map[string]sriracha.RawRecord
}

func (s *sliceSource) Scan(_ context.Context, fn func(string, sriracha.RawRecord) error) error {
	for id, r := range s.records {
		if err := fn(id, r); err != nil {
			return err
		}
	}
	return nil
}

func (s *sliceSource) Fetch(_ context.Context, id string) (sriracha.RawRecord, error) {
	r, ok := s.records[id]
	if !ok {
		return nil, sriracha.ErrNotFound
	}
	return r, nil
}

func TestStats(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("zero on construction", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		s := idx.Stats()
		assert.Equal(t, int64(0), s.RecordCount)
		assert.True(t, s.LastRebuild.IsZero())
		assert.True(t, s.LastSync.IsZero())
	})

	t.Run("updated after Rebuild", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{
			"r1": {sriracha.FieldNameGiven: "Alice", sriracha.FieldNameFamily: "Smith"},
		})))
		s := idx.Stats()
		assert.Equal(t, int64(1), s.RecordCount)
		assert.False(t, s.LastRebuild.IsZero())
	})

	t.Run("updated after Sync", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		require.NoError(t, idx.Sync(ctx, syncSource(t, map[string]sriracha.RawRecord{
			"r1": {sriracha.FieldNameGiven: "Alice", sriracha.FieldNameFamily: "Smith"},
		})))
		s := idx.Stats()
		assert.Equal(t, int64(1), s.RecordCount)
		assert.False(t, s.LastSync.IsZero())
	})

	t.Run("index size bytes populated for BadgerStorage", func(t *testing.T) {
		t.Parallel()
		s, err := OpenBadgerInMemory()
		require.NoError(t, err)
		idx, err := New(s, testFS(), testSecret())
		require.NoError(t, err)
		defer func() { _ = idx.Close() }()

		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{
			"r1": {sriracha.FieldNameGiven: "Alice", sriracha.FieldNameFamily: "Smith"},
		})))
		stats := idx.Stats()
		assert.GreaterOrEqual(t, stats.IndexSizeBytes, int64(0))
	})

	t.Run("close non-closer storage returns nil", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t) // MemoryStorage does not implement io.Closer
		require.NoError(t, idx.Close())
	})
}

func TestDeleteRecord(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	cases := []struct {
		name  string
		setup func(t *testing.T, idx *Indexer, sentinel error)
	}{
		{
			name: "get error",
			setup: func(t *testing.T, idx *Indexer, sentinel error) {
				t.Helper()
				storage := mocksriracha.NewMockIndexStorage(t)
				storage.EXPECT().Get(mock.Anything, "meta:r1").Return(nil, sentinel)
				idx.storage = storage
			},
		},
		{
			name: "delete det error",
			setup: func(t *testing.T, idx *Indexer, sentinel error) {
				t.Helper()
				storage := mocksriracha.NewMockIndexStorage(t)
				storage.EXPECT().Get(mock.Anything, "meta:r1").Return(
					[]byte(`{"det_key":"det:abc","prob_key":"prob:test-v1:r1"}`), nil)
				// Sequential: first Delete fails → no further deletes attempted.
				storage.EXPECT().Delete(mock.Anything, "det:abc").Return(sentinel)
				idx.storage = storage
			},
		},
		{
			name: "delete prob error",
			setup: func(t *testing.T, idx *Indexer, sentinel error) {
				t.Helper()
				storage := mocksriracha.NewMockIndexStorage(t)
				storage.EXPECT().Get(mock.Anything, "meta:r1").Return(
					[]byte(`{"det_key":"det:abc","prob_key":"prob:test-v1:r1"}`), nil)
				// Sequential: det succeeds, prob fails → meta delete not attempted.
				storage.EXPECT().Delete(mock.Anything, "det:abc").Return(nil)
				storage.EXPECT().Delete(mock.Anything, "prob:test-v1:r1").Return(sentinel)
				idx.storage = storage
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sentinel := errors.New("sentinel")
			idx := newTestIndexer(t)
			tc.setup(t, idx, sentinel)
			_, err := idx.deleteRecord(ctx, "r1")
			require.ErrorIs(t, err, sentinel)
		})
	}

	t.Run("nonexistent is no-op", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		found, err := idx.deleteRecord(ctx, "ghost")
		require.NoError(t, err)
		assert.False(t, found, "deleteRecord on non-existent record should return found=false")
	})

	t.Run("malformed meta json", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		require.NoError(t, idx.storage.Put(ctx, "meta:r1", []byte("not-json")))
		_, err := idx.deleteRecord(ctx, "r1")
		require.Error(t, err)
	})

	t.Run("existing removes all three keys", func(t *testing.T) {
		t.Parallel()
		mem := NewMemoryStorage()
		idx, err := New(mem, testFS(), testSecret())
		require.NoError(t, err)
		t.Cleanup(func() { _ = idx.Close() })

		rec := sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Alice",
			sriracha.FieldNameFamily: "Smith",
		}
		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{"r1": rec})))

		var detKeys, probKeys, metaKeys []string
		_ = mem.Scan(ctx, "det:", func(k string, _ []byte) error { detKeys = append(detKeys, k); return nil })
		_ = mem.Scan(ctx, "prob:", func(k string, _ []byte) error { probKeys = append(probKeys, k); return nil })
		_ = mem.Scan(ctx, "meta:", func(k string, _ []byte) error { metaKeys = append(metaKeys, k); return nil })
		assert.Len(t, detKeys, 1)
		assert.Len(t, probKeys, 1)
		assert.Len(t, metaKeys, 1)

		found, err := idx.deleteRecord(ctx, "r1")
		require.NoError(t, err)
		assert.True(t, found, "deleteRecord on existing record should return found=true")

		detKeys, probKeys, metaKeys = nil, nil, nil
		_ = mem.Scan(ctx, "det:", func(k string, _ []byte) error { detKeys = append(detKeys, k); return nil })
		_ = mem.Scan(ctx, "prob:", func(k string, _ []byte) error { probKeys = append(probKeys, k); return nil })
		_ = mem.Scan(ctx, "meta:", func(k string, _ []byte) error { metaKeys = append(metaKeys, k); return nil })
		assert.Empty(t, detKeys)
		assert.Empty(t, probKeys)
		assert.Empty(t, metaKeys)
	})
}
