package sriracha

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordFromMap(t *testing.T) {
	t.Parallel()

	fs := FieldSet{
		Version: "v1",
		Fields: []FieldSpec{
			{Path: FieldNameGiven, Required: true, Weight: 1.0},
			{Path: FieldNameFamily, Required: false, Weight: 1.0},
		},
		ProbabilisticParams: DefaultProbabilisticConfig(),
	}

	t.Run("AllResolve", func(t *testing.T) {
		t.Parallel()
		record, errs := RecordFromMap(map[string]string{
			FieldNameGiven.String():  "Alice",
			FieldNameFamily.String(): "Smith",
		}, fs)
		assert.Empty(t, errs)
		require.Len(t, record, 2)
		assert.Equal(t, "Alice", record[FieldNameGiven])
		assert.Equal(t, "Smith", record[FieldNameFamily])
	})

	t.Run("UnknownPathInFieldSet", func(t *testing.T) {
		t.Parallel()
		record, errs := RecordFromMap(map[string]string{
			FieldNameGiven.String():    "Alice",
			FieldContactEmail.String(): "alice@example.com",
		}, fs)
		require.Len(t, errs, 1)
		assert.Contains(t, errs[0].Error(), "not in FieldSet")
		assert.Equal(t, "Alice", record[FieldNameGiven])
		_, present := record[FieldContactEmail]
		assert.False(t, present, "rejected paths must not appear in the partial record")
	})

	t.Run("MalformedKey", func(t *testing.T) {
		t.Parallel()
		_, errs := RecordFromMap(map[string]string{
			"not-a-path": "x",
		}, fs)
		require.Len(t, errs, 1)
		assert.Contains(t, errs[0].Error(), "not-a-path")
	})

	t.Run("AggregatesAllErrors", func(t *testing.T) {
		t.Parallel()
		_, errs := RecordFromMap(map[string]string{
			"bad1":                     "x",
			FieldContactEmail.String(): "y",
		}, fs)
		require.Len(t, errs, 2, "must surface every error, not just the first")
	})

	t.Run("EmptyInput", func(t *testing.T) {
		t.Parallel()
		record, errs := RecordFromMap(nil, fs)
		assert.Empty(t, errs)
		assert.Empty(t, record)
	})
}
