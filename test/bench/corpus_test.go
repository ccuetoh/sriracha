//go:build bench

package bench

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.sriracha.dev/sriracha"
)

func TestLoadJSONL(t *testing.T) {
	t.Parallel()

	t.Run("ParsesValidCorpus", func(t *testing.T) {
		t.Parallel()
		data := `{"canonical_id":"c1","entity_id":"e1","dataset":"d1","record":{"sriracha::name::given":"Alice","sriracha::name::family":"Smith"}}
{"canonical_id":"c1","entity_id":"e2","dataset":"d1","record":{"sriracha::name::given":"Alice","sriracha::name::family":"Smith"}}

{"canonical_id":"c2","entity_id":"e3","dataset":"d2","record":{"sriracha::name::given":"Bob","sriracha::name::family":"Jones"}}
`
		records, err := readJSONL(bytes.NewReader([]byte(data)), "test")
		require.NoError(t, err)
		require.Len(t, records, 3)

		assert.Equal(t, "c1", records[0].CanonicalID)
		assert.Equal(t, "Alice", records[0].Fields[sriracha.FieldNameGiven])
	})

	t.Run("RejectsMalformedJSON", func(t *testing.T) {
		t.Parallel()
		_, err := readJSONL(bytes.NewReader([]byte("not json\n")), "test")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "line 1")
	})

	t.Run("RejectsMissingCanonicalID", func(t *testing.T) {
		t.Parallel()
		_, err := readJSONL(bytes.NewReader([]byte(`{"entity_id":"e1","record":{}}`+"\n")), "test")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "canonical_id")
	})

	t.Run("RejectsEmptyCorpus", func(t *testing.T) {
		t.Parallel()
		_, err := readJSONL(bytes.NewReader(nil), "test")
		require.Error(t, err)
	})

	t.Run("OpensRealFile", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		path := filepath.Join(dir, "corpus.jsonl")
		err := os.WriteFile(path, []byte(`{"canonical_id":"c1","record":{}}`+"\n"), 0o600)
		require.NoError(t, err)
		records, err := loadJSONL(path)
		require.NoError(t, err)
		assert.Len(t, records, 1)
	})

	t.Run("SurfacesOpenError", func(t *testing.T) {
		t.Parallel()
		_, err := loadJSONL(filepath.Join(t.TempDir(), "does-not-exist.jsonl"))
		require.Error(t, err)
	})
}

func TestSanitize(t *testing.T) {
	t.Parallel()

	t.Run("KeepsValidFieldsDropsInvalid", func(t *testing.T) {
		t.Parallel()
		in := sriracha.RawRecord{
			sriracha.FieldNameGiven:      "Alice",
			sriracha.FieldNameFamily:     "Smith",
			sriracha.FieldAddressCountry: "USA",       // 3-letter, normaliser rejects
			sriracha.FieldDateBirth:      "yesterday", // not ISO 8601
		}
		clean, dropped := sanitize(in)
		assert.Contains(t, clean, sriracha.FieldNameGiven)
		assert.Contains(t, clean, sriracha.FieldNameFamily)
		assert.NotContains(t, clean, sriracha.FieldAddressCountry)
		assert.NotContains(t, clean, sriracha.FieldDateBirth)
		assert.Len(t, dropped, 2)
		assert.NotNil(t, dropped[sriracha.FieldAddressCountry])
		assert.NotNil(t, dropped[sriracha.FieldDateBirth])
	})

	t.Run("AllValidYieldsNilDroppedMap", func(t *testing.T) {
		t.Parallel()
		in := sriracha.RawRecord{
			sriracha.FieldNameGiven: "Alice",
		}
		clean, dropped := sanitize(in)
		assert.Len(t, clean, 1)
		assert.Nil(t, dropped)
	})

	t.Run("EmptyInputYieldsEmptyOutput", func(t *testing.T) {
		t.Parallel()
		clean, dropped := sanitize(sriracha.RawRecord{})
		assert.Empty(t, clean)
		assert.Nil(t, dropped)
	})
}

func TestGroupByCanonical(t *testing.T) {
	t.Parallel()

	records := []record{
		{CanonicalID: "a", EntityID: "1"},
		{CanonicalID: "b", EntityID: "2"},
		{CanonicalID: "a", EntityID: "3"},
		{CanonicalID: "a", EntityID: "4"},
	}
	groups := groupByCanonical(records)
	require.Len(t, groups, 2)
	assert.Equal(t, []int{0, 2, 3}, groups["a"])
	assert.Equal(t, []int{1}, groups["b"])
}
