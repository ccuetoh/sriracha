package indexer

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"

	"go.sriracha.dev/sriracha"
)

const badgerCheckpointKey = "__checkpoint__"

// badgerGCInterval is how often the value-log GC pass runs in the background
// when a persistent store is opened. Badger reclaims space on update/delete
// only when GC is invoked; without this loop a long-running deployment grows
// unboundedly even if the logical record count is stable.
const badgerGCInterval = 5 * time.Minute

// badgerGCDiscardRatio is the minimum fraction of stale data a value-log file
// must contain before Badger rewrites it. 0.5 is the value Badger's docs
// recommend.
const badgerGCDiscardRatio = 0.5

// BadgerStorage implements sriracha.IndexStorage using BadgerDB.
// It also implements io.Closer, StorageSizer, and Transactor.
type BadgerStorage struct {
	db            *badger.DB
	checkpointKey string
	valueCopyFn   func(*badger.Item) ([]byte, error) // nil: use item.ValueCopy(nil); set in tests to inject errors

	gcStop   chan struct{}
	gcDone   chan struct{}
	gcOnce   sync.Once
	closeMu  sync.Mutex
	closed   bool
}

func (s *BadgerStorage) valueCopy(item *badger.Item) ([]byte, error) {
	if s.valueCopyFn != nil {
		return s.valueCopyFn(item)
	}
	return item.ValueCopy(nil)
}

func openBadger(opts badger.Options) (*BadgerStorage, error) {
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	s := &BadgerStorage{
		db:            db,
		checkpointKey: badgerCheckpointKey,
		gcStop:        make(chan struct{}),
		gcDone:        make(chan struct{}),
	}
	// In-memory mode has no value log, so the GC loop is a no-op. Guard
	// against starting it to keep tests deterministic and avoid spurious
	// goroutine leaks.
	if !opts.InMemory {
		go s.runValueLogGC(badgerGCInterval)
	} else {
		close(s.gcDone)
	}
	return s, nil
}

// OpenBadger opens a persistent BadgerDB store at dir.
func OpenBadger(dir string) (*BadgerStorage, error) {
	return openBadger(badger.DefaultOptions(dir).WithLogger(nil))
}

// OpenBadgerInMemory opens an ephemeral in-memory BadgerDB store.
func OpenBadgerInMemory() (*BadgerStorage, error) {
	return openBadger(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
}

// runValueLogGC drives Badger's value-log garbage collector at a fixed
// interval until Close is called. Each tick repeatedly invokes
// db.RunValueLogGC until it reports no rewrite is needed (or any other
// error), so a single pass can reclaim multiple stale files.
func (s *BadgerStorage) runValueLogGC(interval time.Duration) {
	defer close(s.gcDone)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-s.gcStop:
			return
		case <-t.C:
			for {
				if err := s.db.RunValueLogGC(badgerGCDiscardRatio); err != nil {
					break
				}
			}
		}
	}
}

// Close stops the background GC loop and releases all resources held by the
// BadgerDB instance. Calling Close more than once is safe.
func (s *BadgerStorage) Close() error {
	s.closeMu.Lock()
	if s.closed {
		s.closeMu.Unlock()
		return nil
	}
	s.closed = true
	s.closeMu.Unlock()

	s.gcOnce.Do(func() { close(s.gcStop) })
	<-s.gcDone
	return s.db.Close()
}

func (s *BadgerStorage) Put(ctx context.Context, key string, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(key), value)
	})
}

func (s *BadgerStorage) Get(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var val []byte
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, sriracha.ErrNotFound
	}
	return val, nil
}

func (s *BadgerStorage) Scan(ctx context.Context, prefix string, fn func(key string, value []byte) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = []byte(prefix)
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			item := it.Item()
			key := string(item.Key())
			val, err := s.valueCopy(item)
			if err != nil {
				return err
			}
			if err := fn(key, val); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BadgerStorage) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte(key))
	})
}

func (s *BadgerStorage) SaveCheckpoint(ctx context.Context, token string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(badgerCheckpointKey), []byte(token))
	})
}

func (s *BadgerStorage) LoadCheckpoint(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var token string
	err := s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(s.checkpointKey))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		val, err := s.valueCopy(item)
		if err != nil {
			return err
		}
		token = string(val)
		return nil
	})
	return token, err
}

// PutBatch writes all key-value pairs in kvs atomically. Implements Transactor.
func (s *BadgerStorage) PutBatch(ctx context.Context, kvs map[string][]byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		for k, v := range kvs {
			if err := txn.Set([]byte(k), v); err != nil {
				return err
			}
		}
		return nil
	})
}

// DeleteBatch removes all keys atomically. Implements Transactor.
func (s *BadgerStorage) DeleteBatch(ctx context.Context, keys []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.Update(func(txn *badger.Txn) error {
		for _, k := range keys {
			if err := txn.Delete([]byte(k)); err != nil {
				return err
			}
		}
		return nil
	})
}

// SizeBytes returns the combined LSM-tree and value-log size of the BadgerDB instance.
// Implements StorageSizer.
func (s *BadgerStorage) SizeBytes(_ context.Context) (int64, error) {
	lsm, vlog := s.db.Size()
	return lsm + vlog, nil
}
