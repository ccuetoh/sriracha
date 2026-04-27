package sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.sriracha.dev/sriracha"
)

// passThroughConverter lets sqlmock store unconvertible driver.Value entries
// verbatim so tests can drive convertAssign failures inside rows.Scan.
type passThroughConverter struct{}

func (passThroughConverter) ConvertValue(v any) (driver.Value, error) { return v, nil }

func newRawAdapter(t *testing.T, opts ...Option) (Adapter, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual),
		sqlmock.ValueConverterOption(passThroughConverter{}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	base := []Option{
		WithDB(db),
		WithScanQuery(testScanQuery),
		WithFetchQuery(testFetchQuery),
	}
	a, err := New(append(base, opts...)...)
	require.NoError(t, err)
	return a, mock
}

const (
	testScanQuery      = "SELECT __sriracha_record_id, sriracha::name::given AS \"sriracha::name::given\", sriracha::name::family AS \"sriracha::name::family\" FROM persons"
	testFetchQuery     = "SELECT __sriracha_record_id, sriracha::name::given AS \"sriracha::name::given\" FROM persons WHERE id = {sriracha_record_id}"
	testScanSinceQuery = "SELECT __sriracha_record_id, __sriracha_deleted_at, sriracha::name::given AS \"sriracha::name::given\" FROM persons WHERE updated_at > {sriracha_since}"
)

func newMockAdapter(t *testing.T, opts ...Option) (Adapter, sqlmock.Sqlmock) {
	t.Helper()

	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	base := []Option{
		WithDB(db),
		WithScanQuery(testScanQuery),
		WithFetchQuery(testFetchQuery),
	}
	a, err := New(append(base, opts...)...)
	require.NoError(t, err)
	return a, mock
}

func TestNew(t *testing.T) {
	t.Parallel()

	openMockDB := func(t *testing.T) *sql.DB {
		t.Helper()
		db, _, err := sqlmock.New()
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		return db
	}

	cases := []struct {
		name       string
		opts       func(t *testing.T) []Option
		wantErr    bool
		errContain string
	}{
		{
			name: "missing DB",
			opts: func(_ *testing.T) []Option {
				return []Option{
					WithScanQuery(testScanQuery),
					WithFetchQuery(testFetchQuery),
				}
			},
			wantErr:    true,
			errContain: "DB must not be nil",
		},
		{
			name: "missing scan query",
			opts: func(t *testing.T) []Option {
				return []Option{
					WithDB(openMockDB(t)),
					WithFetchQuery(testFetchQuery),
				}
			},
			wantErr:    true,
			errContain: "ScanQuery must not be empty",
		},
		{
			name: "missing fetch query",
			opts: func(t *testing.T) []Option {
				return []Option{
					WithDB(openMockDB(t)),
					WithScanQuery(testScanQuery),
				}
			},
			wantErr:    true,
			errContain: "FetchQuery must not be empty",
		},
		{
			name: "fetch query without record id placeholder",
			opts: func(t *testing.T) []Option {
				return []Option{
					WithDB(openMockDB(t)),
					WithScanQuery(testScanQuery),
					WithFetchQuery("SELECT * FROM persons WHERE id = ?"),
				}
			},
			wantErr:    true,
			errContain: PlaceholderRecordID,
		},
		{
			name: "scan since query without since placeholder",
			opts: func(t *testing.T) []Option {
				return []Option{
					WithDB(openMockDB(t)),
					WithScanQuery(testScanQuery),
					WithFetchQuery(testFetchQuery),
					WithScanSinceQuery("SELECT * FROM persons WHERE updated_at > ?"),
				}
			},
			wantErr:    true,
			errContain: PlaceholderSince,
		},
		{
			name: "nil placeholder function",
			opts: func(t *testing.T) []Option {
				return []Option{
					WithDB(openMockDB(t)),
					WithScanQuery(testScanQuery),
					WithFetchQuery(testFetchQuery),
					WithPlaceholder(nil),
				}
			},
			wantErr:    true,
			errContain: "placeholder function must not be nil",
		},
		{
			name: "fetch query with duplicate record id placeholder",
			opts: func(t *testing.T) []Option {
				return []Option{
					WithDB(openMockDB(t)),
					WithScanQuery(testScanQuery),
					WithFetchQuery("SELECT __sriracha_record_id FROM persons WHERE id = {sriracha_record_id} OR alt_id = {sriracha_record_id}"),
				}
			},
			wantErr:    true,
			errContain: "exactly once",
		},
		{
			name: "scan since query with duplicate since placeholder",
			opts: func(t *testing.T) []Option {
				return []Option{
					WithDB(openMockDB(t)),
					WithScanQuery(testScanQuery),
					WithFetchQuery(testFetchQuery),
					WithScanSinceQuery("SELECT __sriracha_record_id FROM persons WHERE created_at > {sriracha_since} OR updated_at > {sriracha_since}"),
				}
			},
			wantErr:    true,
			errContain: "exactly once",
		},
		{
			name: "scan query with stray record id placeholder",
			opts: func(t *testing.T) []Option {
				return []Option{
					WithDB(openMockDB(t)),
					WithScanQuery("SELECT __sriracha_record_id FROM persons WHERE id = {sriracha_record_id}"),
					WithFetchQuery(testFetchQuery),
				}
			},
			wantErr:    true,
			errContain: "unknown placeholder " + PlaceholderRecordID,
		},
		{
			name: "fetch query with stray since placeholder",
			opts: func(t *testing.T) []Option {
				return []Option{
					WithDB(openMockDB(t)),
					WithScanQuery(testScanQuery),
					WithFetchQuery("SELECT __sriracha_record_id FROM persons WHERE id = {sriracha_record_id} AND created_at > {sriracha_since}"),
				}
			},
			wantErr:    true,
			errContain: "unknown placeholder " + PlaceholderSince,
		},
		{
			name: "scan since query with stray record id placeholder",
			opts: func(t *testing.T) []Option {
				return []Option{
					WithDB(openMockDB(t)),
					WithScanQuery(testScanQuery),
					WithFetchQuery(testFetchQuery),
					WithScanSinceQuery("SELECT __sriracha_record_id FROM persons WHERE updated_at > {sriracha_since} AND id = {sriracha_record_id}"),
				}
			},
			wantErr:    true,
			errContain: "unknown placeholder " + PlaceholderRecordID,
		},
		{
			name: "fetch query with typoed placeholder",
			opts: func(t *testing.T) []Option {
				return []Option{
					WithDB(openMockDB(t)),
					WithScanQuery(testScanQuery),
					WithFetchQuery("SELECT __sriracha_record_id FROM persons WHERE id = {sriracha_id}"),
				}
			},
			wantErr:    true,
			errContain: "unknown placeholder {sriracha_id}",
		},
		{
			name: "scan query missing record id column reference",
			opts: func(t *testing.T) []Option {
				return []Option{
					WithDB(openMockDB(t)),
					WithScanQuery("SELECT id, name FROM persons"),
					WithFetchQuery(testFetchQuery),
				}
			},
			wantErr:    true,
			errContain: "ScanQuery must reference column " + ColumnRecordID,
		},
		{
			name: "fetch query missing record id column reference",
			opts: func(t *testing.T) []Option {
				return []Option{
					WithDB(openMockDB(t)),
					WithScanQuery(testScanQuery),
					WithFetchQuery("SELECT id, name FROM persons WHERE id = {sriracha_record_id}"),
				}
			},
			wantErr:    true,
			errContain: "FetchQuery must reference column " + ColumnRecordID,
		},
		{
			name: "scan since query missing record id column reference",
			opts: func(t *testing.T) []Option {
				return []Option{
					WithDB(openMockDB(t)),
					WithScanQuery(testScanQuery),
					WithFetchQuery(testFetchQuery),
					WithScanSinceQuery("SELECT id, name FROM persons WHERE updated_at > {sriracha_since}"),
				}
			},
			wantErr:    true,
			errContain: "ScanSinceQuery must reference column " + ColumnRecordID,
		},
		{
			name: "happy path with question placeholder",
			opts: func(t *testing.T) []Option {
				return []Option{
					WithDB(openMockDB(t)),
					WithScanQuery(testScanQuery),
					WithFetchQuery(testFetchQuery),
				}
			},
		},
		{
			name: "happy path with dollar placeholder",
			opts: func(t *testing.T) []Option {
				return []Option{
					WithDB(openMockDB(t)),
					WithScanQuery(testScanQuery),
					WithFetchQuery(testFetchQuery),
					WithPlaceholder(PlaceholderDollar),
				}
			},
		},
		{
			name: "happy path with scan since query",
			opts: func(t *testing.T) []Option {
				return []Option{
					WithDB(openMockDB(t)),
					WithScanQuery(testScanQuery),
					WithFetchQuery(testFetchQuery),
					WithScanSinceQuery(testScanSinceQuery),
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			a, err := New(tc.opts(t)...)
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContain)
				assert.Nil(t, a)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, a)
			impl, ok := a.(*adapter)
			require.True(t, ok)
			assert.NotContains(t, impl.fetchQuery, PlaceholderRecordID,
				"placeholder must be substituted at construction time")
			if impl.scanSinceQuery != "" {
				assert.NotContains(t, impl.scanSinceQuery, PlaceholderSince)
			}
		})
	}
}

func TestNew_DollarPlaceholderSubstitution(t *testing.T) {
	t.Parallel()

	db, _, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	a, err := New(
		WithDB(db),
		WithScanQuery(testScanQuery),
		WithFetchQuery("SELECT __sriracha_record_id FROM persons WHERE id = {sriracha_record_id}"),
		WithScanSinceQuery("SELECT __sriracha_record_id FROM persons WHERE updated_at > {sriracha_since}"),
		WithPlaceholder(PlaceholderDollar),
	)
	require.NoError(t, err)

	impl, ok := a.(*adapter)
	require.True(t, ok)
	assert.Equal(t, "SELECT __sriracha_record_id FROM persons WHERE id = $1", impl.fetchQuery)
	assert.Equal(t, "SELECT __sriracha_record_id FROM persons WHERE updated_at > $1", impl.scanSinceQuery)
}

func TestParseColumns(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name             string
		input            []string
		wantErr          bool
		wantRecordIDIdx  int
		wantDeletedAtIdx int
		wantFieldPaths   []string
	}{
		{
			name:             "record id and field paths",
			input:            []string{"__sriracha_record_id", "sriracha::name::given", "sriracha::name::family"},
			wantRecordIDIdx:  0,
			wantDeletedAtIdx: -1,
			wantFieldPaths:   []string{"sriracha::name::given", "sriracha::name::family"},
		},
		{
			name:             "with deleted_at",
			input:            []string{"__sriracha_record_id", "__sriracha_deleted_at", "sriracha::name::given"},
			wantRecordIDIdx:  0,
			wantDeletedAtIdx: 1,
			wantFieldPaths:   []string{"sriracha::name::given"},
		},
		{
			name:             "unrecognised column skipped",
			input:            []string{"__sriracha_record_id", "internal_audit_id", "sriracha::name::given"},
			wantRecordIDIdx:  0,
			wantDeletedAtIdx: -1,
			wantFieldPaths:   []string{"sriracha::name::given"},
		},
		{
			name:    "missing record id column",
			input:   []string{"sriracha::name::given"},
			wantErr: true,
		},
		{
			name:             "record id at non-zero index",
			input:            []string{"sriracha::name::given", "__sriracha_record_id", "sriracha::name::family"},
			wantRecordIDIdx:  1,
			wantDeletedAtIdx: -1,
			wantFieldPaths:   []string{"sriracha::name::given", "sriracha::name::family"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cm, err := parseColumns(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), ColumnRecordID)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantRecordIDIdx, cm.recordIDIndex)
			assert.Equal(t, tc.wantDeletedAtIdx, cm.deletedAtIndex)
			require.Len(t, cm.fields, len(tc.wantFieldPaths))
			for i, want := range tc.wantFieldPaths {
				assert.Equal(t, want, cm.fields[i].path.String())
			}
		})
	}
}

func TestPlaceholderQuestion(t *testing.T) {
	t.Parallel()

	for _, idx := range []int{0, 1, 2, 100} {
		assert.Equal(t, "?", PlaceholderQuestion(idx))
	}
}

func TestPlaceholderDollar(t *testing.T) {
	t.Parallel()

	cases := map[int]string{1: "$1", 2: "$2", 42: "$42"}
	for idx, want := range cases {
		assert.Equal(t, want, PlaceholderDollar(idx))
	}
}

func TestScan(t *testing.T) {
	t.Parallel()

	t.Run("rows including null and unrecognised column", func(t *testing.T) {
		t.Parallel()

		a, mock := newMockAdapter(t)

		rows := sqlmock.NewRows([]string{
			"__sriracha_record_id",
			"sriracha::name::given",
			"sriracha::name::family",
			"internal_audit_id",
		}).
			AddRow("rec-1", "Ada", "Lovelace", "audit-1").
			AddRow("rec-2", "Alan", nil, "audit-2")
		mock.ExpectQuery(testScanQuery).WillReturnRows(rows)

		var seen []string
		var records []sriracha.RawRecord
		err := a.Scan(context.Background(), func(id string, r sriracha.RawRecord) error {
			seen = append(seen, id)
			records = append(records, r)
			return nil
		})
		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())

		assert.Equal(t, []string{"rec-1", "rec-2"}, seen)
		assert.Equal(t, "Ada", records[0][sriracha.FieldNameGiven])
		assert.Equal(t, "Lovelace", records[0][sriracha.FieldNameFamily])
		assert.Equal(t, "Alan", records[1][sriracha.FieldNameGiven])
		assert.Equal(t, string(sriracha.NotFound), records[1][sriracha.FieldNameFamily])
		assert.True(t, sriracha.IsNotFound(records[1][sriracha.FieldNameFamily]))
	})

	t.Run("query error", func(t *testing.T) {
		t.Parallel()

		a, mock := newMockAdapter(t)
		mock.ExpectQuery(testScanQuery).WillReturnError(errors.New("connection refused"))

		err := a.Scan(context.Background(), func(string, sriracha.RawRecord) error { return nil })
		require.Error(t, err)
		assert.Contains(t, err.Error(), "connection refused")
	})

	t.Run("missing record id column", func(t *testing.T) {
		t.Parallel()

		a, mock := newMockAdapter(t)
		rows := sqlmock.NewRows([]string{"sriracha::name::given"}).AddRow("Ada")
		mock.ExpectQuery(testScanQuery).WillReturnRows(rows)

		err := a.Scan(context.Background(), func(string, sriracha.RawRecord) error { return nil })
		require.Error(t, err)
		assert.Contains(t, err.Error(), ColumnRecordID)
	})

	t.Run("null record id row aborts", func(t *testing.T) {
		t.Parallel()

		a, mock := newMockAdapter(t)
		rows := sqlmock.NewRows([]string{"__sriracha_record_id", "sriracha::name::given"}).
			AddRow(nil, "Ada")
		mock.ExpectQuery(testScanQuery).WillReturnRows(rows)

		err := a.Scan(context.Background(), func(string, sriracha.RawRecord) error { return nil })
		require.Error(t, err)
		assert.Contains(t, err.Error(), "record ID")
	})

	t.Run("empty record id row aborts", func(t *testing.T) {
		t.Parallel()

		a, mock := newMockAdapter(t)
		rows := sqlmock.NewRows([]string{"__sriracha_record_id", "sriracha::name::given"}).
			AddRow("", "Ada")
		mock.ExpectQuery(testScanQuery).WillReturnRows(rows)

		err := a.Scan(context.Background(), func(string, sriracha.RawRecord) error { return nil })
		require.Error(t, err)
		assert.Contains(t, err.Error(), "record ID")
	})

	t.Run("callback error stops iteration", func(t *testing.T) {
		t.Parallel()

		a, mock := newMockAdapter(t)
		rows := sqlmock.NewRows([]string{"__sriracha_record_id", "sriracha::name::given"}).
			AddRow("rec-1", "Ada").
			AddRow("rec-2", "Alan")
		mock.ExpectQuery(testScanQuery).WillReturnRows(rows)

		stop := errors.New("callback abort")
		var calls int
		err := a.Scan(context.Background(), func(_ string, _ sriracha.RawRecord) error {
			calls++
			return stop
		})
		require.ErrorIs(t, err, stop)
		assert.Equal(t, 1, calls, "iteration should stop on first callback error")
	})

	t.Run("rows iteration error surfaced", func(t *testing.T) {
		t.Parallel()

		a, mock := newMockAdapter(t)
		boom := errors.New("rows iteration boom")
		rows := sqlmock.NewRows([]string{"__sriracha_record_id", "sriracha::name::given"}).
			AddRow("rec-1", "Ada").
			RowError(0, boom)
		mock.ExpectQuery(testScanQuery).WillReturnRows(rows)

		err := a.Scan(context.Background(), func(string, sriracha.RawRecord) error { return nil })
		require.ErrorIs(t, err, boom)
	})

	t.Run("row scan conversion error", func(t *testing.T) {
		t.Parallel()

		a, mock := newRawAdapter(t)
		rows := mock.NewRows([]string{"__sriracha_record_id", "sriracha::name::given"}).
			AddRow("rec-1", unscannable{})
		mock.ExpectQuery(testScanQuery).WillReturnRows(rows)

		err := a.Scan(context.Background(), func(string, sriracha.RawRecord) error { return nil })
		require.Error(t, err)
	})
}

// unscannable is a driver.Value of kind struct with no fields and no String
// method, so database/sql's convertAssign cannot coerce it into *string.
type unscannable struct{}

func TestFetch(t *testing.T) {
	t.Parallel()

	expectedFetch := strings.ReplaceAll(testFetchQuery, PlaceholderRecordID, "?")

	t.Run("single row with null field", func(t *testing.T) {
		t.Parallel()

		a, mock := newMockAdapter(t)
		rows := sqlmock.NewRows([]string{"__sriracha_record_id", "sriracha::name::given"}).
			AddRow("rec-1", nil)
		mock.ExpectQuery(expectedFetch).WithArgs("rec-1").WillReturnRows(rows)

		r, err := a.Fetch(context.Background(), "rec-1")
		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())
		assert.True(t, sriracha.IsNotFound(r[sriracha.FieldNameGiven]))
	})

	t.Run("single row populated", func(t *testing.T) {
		t.Parallel()

		a, mock := newMockAdapter(t)
		rows := sqlmock.NewRows([]string{"__sriracha_record_id", "sriracha::name::given"}).
			AddRow("rec-1", "Ada")
		mock.ExpectQuery(expectedFetch).WithArgs("rec-1").WillReturnRows(rows)

		r, err := a.Fetch(context.Background(), "rec-1")
		require.NoError(t, err)
		assert.Equal(t, "Ada", r[sriracha.FieldNameGiven])
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		a, mock := newMockAdapter(t)
		rows := sqlmock.NewRows([]string{"__sriracha_record_id", "sriracha::name::given"})
		mock.ExpectQuery(expectedFetch).WithArgs("missing").WillReturnRows(rows)

		r, err := a.Fetch(context.Background(), "missing")
		assert.Nil(t, r)
		require.Error(t, err)
		assert.ErrorIs(t, err, sriracha.ErrRecordNotFound("missing"))
	})

	t.Run("query error", func(t *testing.T) {
		t.Parallel()

		a, mock := newMockAdapter(t)
		mock.ExpectQuery(expectedFetch).WithArgs("rec-1").
			WillReturnError(errors.New("network down"))

		_, err := a.Fetch(context.Background(), "rec-1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "network down")
	})

	t.Run("missing record id column", func(t *testing.T) {
		t.Parallel()

		a, mock := newMockAdapter(t)
		rows := sqlmock.NewRows([]string{"sriracha::name::given"}).AddRow("Ada")
		mock.ExpectQuery(expectedFetch).WithArgs("rec-1").WillReturnRows(rows)

		_, err := a.Fetch(context.Background(), "rec-1")
		require.Error(t, err)
		assert.Contains(t, err.Error(), ColumnRecordID)
	})

	t.Run("rows.Err before any rows", func(t *testing.T) {
		t.Parallel()

		a, mock := newMockAdapter(t)
		boom := errors.New("pre-scan failure")
		rows := sqlmock.NewRows([]string{"__sriracha_record_id", "sriracha::name::given"}).
			AddRow("rec-1", "Ada").
			RowError(0, boom)
		mock.ExpectQuery(expectedFetch).WithArgs("rec-1").WillReturnRows(rows)

		_, err := a.Fetch(context.Background(), "rec-1")
		require.ErrorIs(t, err, boom)
	})

	t.Run("row scan conversion error", func(t *testing.T) {
		t.Parallel()

		a, mock := newRawAdapter(t)
		rows := mock.NewRows([]string{"__sriracha_record_id", "sriracha::name::given"}).
			AddRow("rec-1", unscannable{})
		mock.ExpectQuery(expectedFetch).WithArgs("rec-1").WillReturnRows(rows)

		_, err := a.Fetch(context.Background(), "rec-1")
		require.Error(t, err)
	})
}

func TestScanSince(t *testing.T) {
	t.Parallel()

	expectedSince := strings.ReplaceAll(testScanSinceQuery, PlaceholderSince, "?")

	t.Run("not configured", func(t *testing.T) {
		t.Parallel()

		a, _ := newMockAdapter(t)

		err := a.ScanSince(context.Background(), "checkpoint", func(string, sriracha.RawRecord) error { return nil })
		require.Error(t, err)
		assert.Contains(t, err.Error(), "ScanSinceQuery not configured")
	})

	t.Run("normal and deleted rows", func(t *testing.T) {
		t.Parallel()

		a, mock := newMockAdapter(t, WithScanSinceQuery(testScanSinceQuery))

		rows := sqlmock.NewRows([]string{"__sriracha_record_id", "__sriracha_deleted_at", "sriracha::name::given"}).
			AddRow("rec-1", nil, "Ada").
			AddRow("rec-2", "2026-04-27T00:00:00Z", nil).
			AddRow("rec-3", nil, "Grace")
		mock.ExpectQuery(expectedSince).WithArgs("ckpt-1").WillReturnRows(rows)

		type emit struct {
			id string
			r  sriracha.RawRecord
		}
		var emits []emit
		err := a.ScanSince(context.Background(), "ckpt-1", func(id string, r sriracha.RawRecord) error {
			emits = append(emits, emit{id: id, r: r})
			return nil
		})
		require.NoError(t, err)
		require.NoError(t, mock.ExpectationsWereMet())

		require.Len(t, emits, 3)
		assert.Equal(t, "rec-1", emits[0].id)
		assert.Equal(t, "Ada", emits[0].r[sriracha.FieldNameGiven])
		assert.Equal(t, "rec-2", emits[1].id)
		assert.Nil(t, emits[1].r, "deleted record must surface as nil RawRecord")
		assert.Equal(t, "rec-3", emits[2].id)
		assert.Equal(t, "Grace", emits[2].r[sriracha.FieldNameGiven])
	})

	t.Run("without deleted_at column treats all as live", func(t *testing.T) {
		t.Parallel()

		a, mock := newMockAdapter(t, WithScanSinceQuery(testScanSinceQuery))

		rows := sqlmock.NewRows([]string{"__sriracha_record_id", "sriracha::name::given"}).
			AddRow("rec-1", "Ada")
		mock.ExpectQuery(expectedSince).WithArgs("ckpt").WillReturnRows(rows)

		var calls int
		err := a.ScanSince(context.Background(), "ckpt", func(_ string, r sriracha.RawRecord) error {
			calls++
			assert.NotNil(t, r)
			return nil
		})
		require.NoError(t, err)
		assert.Equal(t, 1, calls)
	})

	t.Run("query error", func(t *testing.T) {
		t.Parallel()

		a, mock := newMockAdapter(t, WithScanSinceQuery(testScanSinceQuery))
		mock.ExpectQuery(expectedSince).WithArgs("ckpt").
			WillReturnError(errors.New("timeout"))

		err := a.ScanSince(context.Background(), "ckpt", func(string, sriracha.RawRecord) error { return nil })
		require.Error(t, err)
		assert.Contains(t, err.Error(), "timeout")
	})

	t.Run("missing record id column", func(t *testing.T) {
		t.Parallel()

		a, mock := newMockAdapter(t, WithScanSinceQuery(testScanSinceQuery))
		rows := sqlmock.NewRows([]string{"sriracha::name::given"}).AddRow("Ada")
		mock.ExpectQuery(expectedSince).WithArgs("ckpt").WillReturnRows(rows)

		err := a.ScanSince(context.Background(), "ckpt", func(string, sriracha.RawRecord) error { return nil })
		require.Error(t, err)
		assert.Contains(t, err.Error(), ColumnRecordID)
	})

	t.Run("null record id row aborts", func(t *testing.T) {
		t.Parallel()

		a, mock := newMockAdapter(t, WithScanSinceQuery(testScanSinceQuery))
		rows := sqlmock.NewRows([]string{"__sriracha_record_id", "sriracha::name::given"}).
			AddRow(nil, "Ada")
		mock.ExpectQuery(expectedSince).WithArgs("ckpt").WillReturnRows(rows)

		err := a.ScanSince(context.Background(), "ckpt", func(string, sriracha.RawRecord) error { return nil })
		require.Error(t, err)
		assert.Contains(t, err.Error(), "record ID")
	})

	t.Run("callback error on deletion stops iteration", func(t *testing.T) {
		t.Parallel()

		a, mock := newMockAdapter(t, WithScanSinceQuery(testScanSinceQuery))
		rows := sqlmock.NewRows([]string{"__sriracha_record_id", "__sriracha_deleted_at", "sriracha::name::given"}).
			AddRow("rec-1", "2026-04-27T00:00:00Z", nil).
			AddRow("rec-2", nil, "Ada")
		mock.ExpectQuery(expectedSince).WithArgs("ckpt").WillReturnRows(rows)

		stop := errors.New("stop on delete")
		var calls int
		err := a.ScanSince(context.Background(), "ckpt", func(_ string, _ sriracha.RawRecord) error {
			calls++
			return stop
		})
		require.ErrorIs(t, err, stop)
		assert.Equal(t, 1, calls)
	})

	t.Run("callback error on live row stops iteration", func(t *testing.T) {
		t.Parallel()

		a, mock := newMockAdapter(t, WithScanSinceQuery(testScanSinceQuery))
		rows := sqlmock.NewRows([]string{"__sriracha_record_id", "sriracha::name::given"}).
			AddRow("rec-1", "Ada").
			AddRow("rec-2", "Alan")
		mock.ExpectQuery(expectedSince).WithArgs("ckpt").WillReturnRows(rows)

		stop := errors.New("stop on live")
		var calls int
		err := a.ScanSince(context.Background(), "ckpt", func(_ string, _ sriracha.RawRecord) error {
			calls++
			return stop
		})
		require.ErrorIs(t, err, stop)
		assert.Equal(t, 1, calls)
	})

	t.Run("rows iteration error surfaced", func(t *testing.T) {
		t.Parallel()

		a, mock := newMockAdapter(t, WithScanSinceQuery(testScanSinceQuery))
		boom := errors.New("scan-since boom")
		rows := sqlmock.NewRows([]string{"__sriracha_record_id", "sriracha::name::given"}).
			AddRow("rec-1", "Ada").
			RowError(0, boom)
		mock.ExpectQuery(expectedSince).WithArgs("ckpt").WillReturnRows(rows)

		err := a.ScanSince(context.Background(), "ckpt", func(string, sriracha.RawRecord) error { return nil })
		require.ErrorIs(t, err, boom)
	})

	t.Run("row scan conversion error", func(t *testing.T) {
		t.Parallel()

		a, mock := newRawAdapter(t, WithScanSinceQuery(testScanSinceQuery))
		rows := mock.NewRows([]string{"__sriracha_record_id", "sriracha::name::given"}).
			AddRow("rec-1", unscannable{})
		mock.ExpectQuery(expectedSince).WithArgs("ckpt").WillReturnRows(rows)

		err := a.ScanSince(context.Background(), "ckpt", func(string, sriracha.RawRecord) error { return nil })
		require.Error(t, err)
	})
}

func TestAdapter_IncrementalRecordSourceCompliance(t *testing.T) {
	t.Parallel()

	a, _ := newMockAdapter(t, WithScanSinceQuery(testScanSinceQuery))
	var _ sriracha.RecordSource = a
	var _ sriracha.IncrementalRecordSource = a
}

func BenchmarkScan(b *testing.B) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	a, err := New(
		WithDB(db),
		WithScanQuery(testScanQuery),
		WithFetchQuery(testFetchQuery),
	)
	if err != nil {
		b.Fatal(err)
	}

	noop := func(string, sriracha.RawRecord) error { return nil }
	for b.Loop() {
		rows := sqlmock.NewRows([]string{"__sriracha_record_id", "sriracha::name::given", "sriracha::name::family"})
		for i := 0; i < 1000; i++ {
			rows.AddRow(fmt.Sprintf("rec-%d", i), "Ada", "Lovelace")
		}
		mock.ExpectQuery(testScanQuery).WillReturnRows(rows)

		if err := a.Scan(context.Background(), noop); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFetch(b *testing.B) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	a, err := New(
		WithDB(db),
		WithScanQuery(testScanQuery),
		WithFetchQuery(testFetchQuery),
	)
	if err != nil {
		b.Fatal(err)
	}

	expected := strings.ReplaceAll(testFetchQuery, PlaceholderRecordID, "?")

	for b.Loop() {
		rows := sqlmock.NewRows([]string{"__sriracha_record_id", "sriracha::name::given"}).
			AddRow("rec-1", "Ada")
		mock.ExpectQuery(expected).WithArgs("rec-1").WillReturnRows(rows)

		if _, err := a.Fetch(context.Background(), "rec-1"); err != nil {
			b.Fatal(err)
		}
	}
}

func FuzzParseColumns(f *testing.F) {
	seeds := []string{
		"",
		"__sriracha_record_id",
		"sriracha::name::given",
		"__sriracha_deleted_at",
		"weird::column",
		"::::",
		"a::b::c",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, name string) {
		_, _ = parseColumns([]string{name})
		_, _ = parseColumns([]string{ColumnRecordID, name})
		_, _ = parseColumns([]string{ColumnRecordID, ColumnDeletedAt, name})
	})
}
