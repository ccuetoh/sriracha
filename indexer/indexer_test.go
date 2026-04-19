package indexer

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

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
				assert.NotNil(t, idx)
			}
		})
	}

	t.Run("loads existing stats", func(t *testing.T) {
		t.Parallel()
		mem := NewMemoryStorage()
		idx, err := New(mem, testFS(), testSecret())
		require.NoError(t, err)
		require.NoError(t, idx.Rebuild(context.Background(), scanSource(t, map[string]sriracha.RawRecord{
			"r1": {sriracha.FieldNameGiven: "Alice", sriracha.FieldNameFamily: "Smith"},
		})))
		idx2, err := New(mem, testFS(), testSecret())
		require.NoError(t, err)
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
			name: "index record error",
			setup: func(t *testing.T, idx *Indexer, sentinel error) sriracha.RecordSource {
				t.Helper()
				storage := mocksriracha.NewMockIndexStorage(t)
				storage.EXPECT().Scan(mock.Anything, "det:", mock.Anything).Return(nil)
				storage.EXPECT().Scan(mock.Anything, "prob:", mock.Anything).Return(nil)
				storage.EXPECT().Scan(mock.Anything, "meta:", mock.Anything).Return(nil)
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
		require.NoError(t, idx.Rebuild(ctx, scanSource(t, map[string]sriracha.RawRecord{
			"r1": {sriracha.FieldNameGiven: "Alice", sriracha.FieldNameFamily: "Smith"},
		})))

		metaBytes, getErr := mem.Get(ctx, "meta:r1")
		require.NoError(t, getErr)

		sentinel := errors.New("delete fail")
		storage := mocksriracha.NewMockIndexStorage(t)
		storage.EXPECT().LoadCheckpoint(mock.Anything).Return("", nil)
		storage.EXPECT().Get(mock.Anything, "meta:r1").Return(metaBytes, nil)
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
			name:        "payload length mismatch error",
			setupQuery:  func() []byte { return make([]byte, fieldBytes) },
			setupStored: func() []byte { return make([]byte, totalBytes) },
			wantErr:     true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			score, err := scoreProbabilistic(tc.setupQuery(), tc.setupStored(), fs, sriracha.MatchConfig{})
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
			err := idx.deleteRecord(ctx, "r1")
			require.ErrorIs(t, err, sentinel)
		})
	}

	t.Run("nonexistent is no-op", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		require.NoError(t, idx.deleteRecord(ctx, "ghost"))
	})

	t.Run("malformed meta json", func(t *testing.T) {
		t.Parallel()
		idx := newTestIndexer(t)
		require.NoError(t, idx.storage.Put(ctx, "meta:r1", []byte("not-json")))
		err := idx.deleteRecord(ctx, "r1")
		require.Error(t, err)
	})

	t.Run("existing removes all three keys", func(t *testing.T) {
		t.Parallel()
		mem := NewMemoryStorage()
		idx, err := New(mem, testFS(), testSecret())
		require.NoError(t, err)

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

		require.NoError(t, idx.deleteRecord(ctx, "r1"))

		detKeys, probKeys, metaKeys = nil, nil, nil
		_ = mem.Scan(ctx, "det:", func(k string, _ []byte) error { detKeys = append(detKeys, k); return nil })
		_ = mem.Scan(ctx, "prob:", func(k string, _ []byte) error { probKeys = append(probKeys, k); return nil })
		_ = mem.Scan(ctx, "meta:", func(k string, _ []byte) error { metaKeys = append(metaKeys, k); return nil })
		assert.Empty(t, detKeys)
		assert.Empty(t, probKeys)
		assert.Empty(t, metaKeys)
	})
}
