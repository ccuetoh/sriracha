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
		record, err := RecordFromMap(map[string]string{
			FieldNameGiven.String():  "Alice",
			FieldNameFamily.String(): "Smith",
		}, fs)
		require.NoError(t, err)
		require.Len(t, record, 2)
		assert.Equal(t, "Alice", record[FieldNameGiven])
		assert.Equal(t, "Smith", record[FieldNameFamily])
	})

	t.Run("UnknownPathInFieldSet", func(t *testing.T) {
		t.Parallel()
		record, err := RecordFromMap(map[string]string{
			FieldNameGiven.String():    "Alice",
			FieldContactEmail.String(): "alice@example.com",
		}, fs)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrUnknownField)

		var fieldErr FieldError
		require.ErrorAs(t, err, &fieldErr)
		assert.Equal(t, FieldContactEmail, fieldErr.Path, "errors.As must recover the rejected path")

		assert.Equal(t, "Alice", record[FieldNameGiven])
		_, present := record[FieldContactEmail]
		assert.False(t, present, "rejected paths must not appear in the partial record")
	})

	t.Run("MalformedKey", func(t *testing.T) {
		t.Parallel()
		_, err := RecordFromMap(map[string]string{
			"not-a-path": "x",
		}, fs)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrInvalidFieldPath)

		var fieldErr FieldError
		require.ErrorAs(t, err, &fieldErr)
		assert.Equal(t, "not-a-path", fieldErr.Path.String(), "the raw key is preserved on the leaf")
		assert.Contains(t, err.Error(), "not-a-path")
	})

	t.Run("AggregatesAllErrors", func(t *testing.T) {
		t.Parallel()
		_, err := RecordFromMap(map[string]string{
			"bad1":                     "x",
			FieldContactEmail.String(): "y",
		}, fs)
		require.Error(t, err)

		joined, ok := err.(interface{ Unwrap() []error })
		require.True(t, ok, "the result must be an errors.Join")
		leaves := joined.Unwrap()
		require.Len(t, leaves, 2, "must surface every error, not just the first")

		assert.ErrorIs(t, leaves[0], ErrInvalidFieldPath, "sorted key order puts the malformed key first")
		assert.ErrorIs(t, leaves[1], ErrUnknownField)
		assert.ErrorIs(t, err, ErrInvalidFieldPath, "every leaf stays reachable through the join")
		assert.ErrorIs(t, err, ErrUnknownField)
	})

	t.Run("EmptyInput", func(t *testing.T) {
		t.Parallel()
		record, err := RecordFromMap(nil, fs)
		assert.NoError(t, err)
		assert.Empty(t, record)
	})
}
