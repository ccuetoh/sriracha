// Package sql implements sriracha.RecordSource and sriracha.IncrementalRecordSource
// backed by a database/sql connection.
//
// Import path: go.sriracha.dev/adapter/sql
//
// The adapter is driver-agnostic: institutions provide a *sql.DB and the SQL
// query templates that surface their record schema. Sriracha never generates
// SQL on its own — the institution remains in full control of which columns
// are exposed and how the underlying tables are queried.
//
// # Column convention
//
// The adapter inspects the result-set column names returned by each query.
// Two reserved names carry adapter metadata; every other column name is parsed
// as a sriracha.FieldPath ("<org>::<namespace>::<name>") and unrecognised
// columns are silently skipped:
//
//   - __sriracha_record_id   the record identifier (required, must be non-NULL)
//   - __sriracha_deleted_at  optional tombstone timestamp for ScanSince
//
// # Query templates
//
// Three query strings are accepted:
//
//   - ScanQuery       full-table scan; runs without parameters.
//   - FetchQuery      single-row lookup; must contain {sriracha_record_id}.
//   - ScanSinceQuery  optional incremental scan; must contain {sriracha_since}.
//
// Each {sriracha_record_id} or {sriracha_since} placeholder is substituted at
// construction time with the parameter marker produced by WithPlaceholder
// (defaults to "?" for MySQL/SQLite; use PlaceholderDollar for PostgreSQL).
// Values are always passed as parameter arguments — never interpolated into
// the SQL string — so user data cannot influence the query plan.
//
// # NULL handling
//
// NULL values in non-reserved columns are surfaced to the indexer as the
// sriracha.NotFound sentinel. A NULL or empty record-ID column produces an
// error and aborts the scan.
package sql
