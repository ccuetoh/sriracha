package sriracha

import "context"

// RecordSource provides access to raw records.
type RecordSource interface {
	// Scan iterates all records, calling fn for each. Stops on first error.
	Scan(ctx context.Context, fn func(id string, r RawRecord) error) error
	// Fetch retrieves a single record by ID.
	Fetch(ctx context.Context, id string) (RawRecord, error)
}

// IncrementalRecordSource extends RecordSource with incremental scanning.
type IncrementalRecordSource interface {
	RecordSource
	// ScanSince iterates records modified after the given checkpoint token.
	ScanSince(ctx context.Context, checkpoint string, fn func(id string, r RawRecord) error) error
}

// TokenIndexer manages a searchable token index.
type TokenIndexer interface {
	// Rebuild reindexes all records from the source.
	Rebuild(ctx context.Context, src RecordSource) error
	// Sync applies incremental updates from the source.
	Sync(ctx context.Context, src IncrementalRecordSource) error
	// Match searches the index for candidates matching the given token record.
	Match(ctx context.Context, tr TokenRecord, cfg MatchConfig) ([]Candidate, error)
	// Stats returns current index statistics.
	Stats() IndexStats
}

// IndexStorage provides low-level key-value storage for the token index.
type IndexStorage interface {
	// Put stores a value under key.
	Put(ctx context.Context, key string, value []byte) error
	// Get retrieves the value for key.
	// Returns nil, sriracha.ErrNotFound if the key does not exist.
	Get(ctx context.Context, key string) ([]byte, error)
	// Scan iterates all keys with the given prefix.
	Scan(ctx context.Context, prefix string, fn func(key string, value []byte) error) error
	// Delete removes the entry for key.
	Delete(ctx context.Context, key string) error
	// SaveCheckpoint persists an opaque checkpoint token.
	SaveCheckpoint(ctx context.Context, token string) error
	// LoadCheckpoint retrieves the most recently saved checkpoint token.
	LoadCheckpoint(ctx context.Context) (string, error)
}

// AuditLog records protocol events for compliance.
type AuditLog interface {
	// Append adds an event to the log, computing and setting PreviousHash.
	// The caller provides all fields except PreviousHash and EventID;
	// the implementation fills those in.
	Append(ctx context.Context, event AuditEvent) error
	// Verify checks hash chain integrity. Returns nil if the chain is intact.
	Verify(ctx context.Context) error
}
