package sriracha

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFieldError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		err     FieldError
		want    string
		wantIs  error
		wantNil bool
	}{
		{
			name:   "SentinelLeaf",
			err:    FieldError{Path: FieldNameGiven, Err: ErrRequiredFieldMissing},
			want:   `field "sriracha::name::given": required field missing`,
			wantIs: ErrRequiredFieldMissing,
		},
		{
			name:   "WrappedLeaf",
			err:    FieldError{Path: FieldDateBirth, Err: fmt.Errorf("%w: parse failed", ErrEmptyValue)},
			want:   `field "sriracha::date::birth": value is empty: parse failed`,
			wantIs: ErrEmptyValue,
		},
		{
			name:   "RawKeyPath",
			err:    FieldError{Path: FieldPath{raw: "not-a-path"}, Err: ErrInvalidFieldPath},
			want:   `field "not-a-path": invalid field path`,
			wantIs: ErrInvalidFieldPath,
		},
		{
			name:    "NilInner",
			err:     FieldError{Path: FieldNameGiven},
			want:    `field "sriracha::name::given": <nil>`,
			wantNil: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.err.Error())
			if tc.wantNil {
				assert.NoError(t, tc.err.Unwrap())
				return
			}
			assert.ErrorIs(t, tc.err, tc.wantIs)
			assert.Equal(t, tc.err.Err, tc.err.Unwrap())
		})
	}
}

func TestFieldErrorRecoversPathThroughJoin(t *testing.T) {
	t.Parallel()

	joined := errors.Join(
		FieldError{Path: FieldNameGiven, Err: ErrRequiredFieldMissing},
		FieldError{Path: FieldContactEmail, Err: ErrUnknownField},
	)

	var got FieldError
	require.ErrorAs(t, joined, &got)
	assert.Equal(t, FieldNameGiven, got.Path, "errors.As finds the first leaf")
	assert.ErrorIs(t, joined, ErrRequiredFieldMissing)
	assert.ErrorIs(t, joined, ErrUnknownField)
}

func TestSentinelsAreDistinct(t *testing.T) {
	t.Parallel()

	all := []error{
		ErrInvalidFieldPath,
		ErrUnknownField,
		ErrDuplicateField,
		ErrRequiredFieldMissing,
		ErrEmptyValue,
		ErrInvalidWeight,
		ErrMissingVersion,
		ErrInvalidConfig,
	}
	seen := make(map[string]struct{}, len(all))
	for _, err := range all {
		require.Error(t, err)
		_, dup := seen[err.Error()]
		assert.Falsef(t, dup, "duplicate sentinel message %q", err.Error())
		seen[err.Error()] = struct{}{}
	}
}
