package indexer

import (
	"context"
	"errors"

	"github.com/dgraph-io/badger/v4"

	"go.sriracha.dev/sriracha"
)

const badgerCheckpointKey = "__checkpoint__"

// BadgerStorage implements sriracha.IndexStorage using BadgerDB.
// It also implements io.Closer, StorageSizer, and Transactor.
type BadgerStorage struct {
	db            *badger.DB
	checkpointKey string
	valueCopyFn   func(*badger.Item) ([]byte, error) // nil: use item.ValueCopy(nil); set in tests to inject errors
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
	return &BadgerStorage{db: db, checkpointKey: badgerCheckpointKey}, nil
}

// OpenBadger opens a persistent BadgerDB store at dir.
func OpenBadger(dir string) (*BadgerStorage, error) {
	return openBadger(badger.DefaultOptions(dir).WithLogger(nil))
}

// OpenBadgerInMemory opens an ephemeral in-memory BadgerDB store.
func OpenBadgerInMemory() (*BadgerStorage, error) {
	return openBadger(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
}

// Close releases all resources held by the BadgerDB instance.
func (s *BadgerStorage) Close() error {
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
