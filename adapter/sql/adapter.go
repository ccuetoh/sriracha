package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"go.sriracha.dev/sriracha"
)

const (
	// ColumnRecordID is the reserved column name carrying the record identifier.
	ColumnRecordID = "__sriracha_record_id"
	// ColumnDeletedAt is the reserved column name carrying an optional deletion
	// tombstone for ScanSince. A non-NULL value signals the indexer to remove
	// the record from the index.
	ColumnDeletedAt = "__sriracha_deleted_at"

	// PlaceholderRecordID is replaced in FetchQuery with the driver-specific
	// parameter marker produced by the configured placeholder function.
	PlaceholderRecordID = "{sriracha_record_id}"
	// PlaceholderSince is replaced in ScanSinceQuery with the driver-specific
	// parameter marker produced by the configured placeholder function.
	PlaceholderSince = "{sriracha_since}"
)

// PlaceholderQuestion returns "?" for any index. Used by MySQL and SQLite.
func PlaceholderQuestion(_ int) string { return "?" }

// PlaceholderDollar returns "$1", "$2", ... — the parameter marker used by PostgreSQL.
func PlaceholderDollar(index int) string { return "$" + strconv.Itoa(index) }

// Adapter exposes a database/sql connection as a sriracha.RecordSource and,
// when ScanSinceQuery is configured, a sriracha.IncrementalRecordSource.
type Adapter struct {
	db             *sql.DB
	scanQuery      string
	fetchQuery     string
	scanSinceQuery string
	placeholder    func(index int) string
}

// Option configures an Adapter. Pass options to New.
type Option func(*Adapter)

// WithDB sets the database handle. Required.
func WithDB(db *sql.DB) Option {
	return func(a *Adapter) { a.db = db }
}

// WithScanQuery sets the full-table scan query. Required.
// The query takes no parameters and must produce a column named
// __sriracha_record_id alongside any field-path columns.
func WithScanQuery(q string) Option {
	return func(a *Adapter) { a.scanQuery = q }
}

// WithFetchQuery sets the single-record lookup query. Required.
// The query must contain the {sriracha_record_id} placeholder, which is
// replaced with the driver-specific parameter marker at construction time.
func WithFetchQuery(q string) Option {
	return func(a *Adapter) { a.fetchQuery = q }
}

// WithScanSinceQuery sets the incremental scan query. Optional; when omitted
// ScanSince returns an error. The query must contain the {sriracha_since}
// placeholder and may produce a __sriracha_deleted_at column to signal
// deletions to the indexer.
func WithScanSinceQuery(q string) Option {
	return func(a *Adapter) { a.scanSinceQuery = q }
}

// WithPlaceholder overrides the parameter-marker generator. Defaults to
// PlaceholderQuestion. Use PlaceholderDollar for PostgreSQL.
func WithPlaceholder(fn func(index int) string) Option {
	return func(a *Adapter) { a.placeholder = fn }
}

var _ sriracha.IncrementalRecordSource = (*Adapter)(nil)

// New constructs an Adapter from the supplied options.
//
// WithDB, WithScanQuery, and WithFetchQuery are required. WithScanSinceQuery
// is optional; without it ScanSince returns an error. WithPlaceholder defaults
// to PlaceholderQuestion (MySQL, SQLite).
func New(opts ...Option) (*Adapter, error) {
	a := &Adapter{placeholder: PlaceholderQuestion}
	for _, opt := range opts {
		opt(a)
	}

	if a.db == nil {
		return nil, errors.New("adapter/sql: DB must not be nil")
	}
	if a.scanQuery == "" {
		return nil, errors.New("adapter/sql: ScanQuery must not be empty")
	}
	if a.fetchQuery == "" {
		return nil, errors.New("adapter/sql: FetchQuery must not be empty")
	}
	if a.placeholder == nil {
		return nil, errors.New("adapter/sql: placeholder function must not be nil")
	}
	if !strings.Contains(a.fetchQuery, PlaceholderRecordID) {
		return nil, fmt.Errorf("adapter/sql: FetchQuery must contain %s placeholder", PlaceholderRecordID)
	}
	if a.scanSinceQuery != "" && !strings.Contains(a.scanSinceQuery, PlaceholderSince) {
		return nil, fmt.Errorf("adapter/sql: ScanSinceQuery must contain %s placeholder", PlaceholderSince)
	}

	a.fetchQuery = strings.ReplaceAll(a.fetchQuery, PlaceholderRecordID, a.placeholder(1))
	if a.scanSinceQuery != "" {
		a.scanSinceQuery = strings.ReplaceAll(a.scanSinceQuery, PlaceholderSince, a.placeholder(1))
	}

	return a, nil
}

type columnField struct {
	index int
	path  sriracha.FieldPath
}

type columnMap struct {
	recordIDIndex  int
	deletedAtIndex int
	fields         []columnField
}

// parseColumns inspects result-set column names and classifies each one.
// Reserved names populate the record-ID and deleted-at indices; remaining
// names are parsed as FieldPaths. Unrecognised names are skipped silently so
// institutions can SELECT extra bookkeeping columns without configuration.
// Returns an error only when the record-ID column is absent.
func parseColumns(names []string) (columnMap, error) {
	cm := columnMap{recordIDIndex: -1, deletedAtIndex: -1}
	for i, name := range names {
		switch name {
		case ColumnRecordID:
			cm.recordIDIndex = i
		case ColumnDeletedAt:
			cm.deletedAtIndex = i
		default:
			fp, err := sriracha.ParseFieldPath(name)
			if err != nil {
				continue
			}
			cm.fields = append(cm.fields, columnField{index: i, path: fp})
		}
	}

	if cm.recordIDIndex == -1 {
		return columnMap{}, fmt.Errorf("adapter/sql: result set missing %s column", ColumnRecordID)
	}

	return cm, nil
}

// buildRecord assembles a RawRecord from a row's scanned values. NULL values
// in field columns are surfaced as the sriracha.NotFound sentinel.
func buildRecord(cm columnMap, values []sql.NullString) sriracha.RawRecord {
	r := make(sriracha.RawRecord, len(cm.fields))
	for _, f := range cm.fields {
		v := values[f.index]
		if v.Valid {
			r[f.path] = v.String
		} else {
			r[f.path] = string(sriracha.NotFound)
		}
	}
	return r
}

// Scan iterates every row produced by the configured ScanQuery and invokes fn
// for each record. Iteration stops on the first error returned by fn.
func (a *Adapter) Scan(ctx context.Context, fn func(id string, r sriracha.RawRecord) error) error {
	rows, err := a.db.QueryContext(ctx, a.scanQuery)
	if err != nil {
		return err
	}
	defer rows.Close()

	names, err := rows.Columns()
	if err != nil {
		return err
	}

	cm, err := parseColumns(names)
	if err != nil {
		return err
	}

	values := make([]sql.NullString, len(names))
	dest := make([]any, len(names))
	for i := range values {
		dest[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return err
		}

		idVal := values[cm.recordIDIndex]
		if !idVal.Valid || idVal.String == "" {
			return errors.New("adapter/sql: record ID column is NULL or empty")
		}

		if err := fn(idVal.String, buildRecord(cm, values)); err != nil {
			return err
		}
	}

	return rows.Err()
}

// Fetch retrieves a single record by ID using the configured FetchQuery.
// Returns sriracha.ErrRecordNotFound when the query yields no rows.
func (a *Adapter) Fetch(ctx context.Context, id string) (sriracha.RawRecord, error) {
	rows, err := a.db.QueryContext(ctx, a.fetchQuery, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	names, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	cm, err := parseColumns(names)
	if err != nil {
		return nil, err
	}

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, sriracha.ErrRecordNotFound(id)
	}

	values := make([]sql.NullString, len(names))
	dest := make([]any, len(names))
	for i := range values {
		dest[i] = &values[i]
	}

	if err := rows.Scan(dest...); err != nil {
		return nil, err
	}

	return buildRecord(cm, values), nil
}

// ScanSince iterates rows modified after the supplied checkpoint token.
// Records whose __sriracha_deleted_at column is non-NULL are surfaced to fn
// with a nil RawRecord, signalling deletion to the indexer.
//
// Returns an error if the adapter was constructed without ScanSinceQuery.
func (a *Adapter) ScanSince(ctx context.Context, checkpoint string, fn func(id string, r sriracha.RawRecord) error) error {
	if a.scanSinceQuery == "" {
		return errors.New("adapter/sql: ScanSinceQuery not configured")
	}

	rows, err := a.db.QueryContext(ctx, a.scanSinceQuery, checkpoint)
	if err != nil {
		return err
	}
	defer rows.Close()

	names, err := rows.Columns()
	if err != nil {
		return err
	}

	cm, err := parseColumns(names)
	if err != nil {
		return err
	}

	values := make([]sql.NullString, len(names))
	dest := make([]any, len(names))
	for i := range values {
		dest[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return err
		}

		idVal := values[cm.recordIDIndex]
		if !idVal.Valid || idVal.String == "" {
			return errors.New("adapter/sql: record ID column is NULL or empty")
		}

		if cm.deletedAtIndex != -1 && values[cm.deletedAtIndex].Valid {
			if err := fn(idVal.String, nil); err != nil {
				return err
			}
			continue
		}

		if err := fn(idVal.String, buildRecord(cm, values)); err != nil {
			return err
		}
	}

	return rows.Err()
}
