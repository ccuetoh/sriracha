package sriracha

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrConstructors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      *Error
		wantCode ErrorCode
	}{
		{name: "ErrTokenMalformed", err: ErrTokenMalformed(FieldNameGiven), wantCode: CodeTokenMalformed},
		{name: "ErrFieldSetIncompatible", err: ErrFieldSetIncompatible("v1.0", "v2.0"), wantCode: CodeFieldSetIncompatible},
		{name: "ErrNormalizationFailed", err: ErrNormalizationFailed(FieldNameFamily, "invalid UTF-8"), wantCode: CodeNormalizationFailed},
		{name: "ErrChecksumMismatch", err: ErrChecksumMismatch(), wantCode: CodeChecksumMismatch},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.NotNil(t, tt.err)
			assert.Equal(t, tt.wantCode, tt.err.Code, "Code")
			assert.NotEmpty(t, tt.err.Error(), "Error() should not return empty string")
		})
	}
}

func TestError_Is(t *testing.T) {
	t.Parallel()

	err := ErrChecksumMismatch()
	assert.ErrorIs(t, err, ErrChecksumMismatch(), "should match same ErrorCode")
	assert.NotErrorIs(t, err, ErrTokenMalformed(FieldNameGiven), "should not match different ErrorCode")
	assert.NotErrorIs(t, err, fmt.Errorf("plain error"), "should return false for non-*Error target")
}

func TestError_Unwrap(t *testing.T) {
	t.Parallel()

	cause := ErrChecksumMismatch()
	wrapped := &Error{Code: CodeTokenMalformed, Message: "outer", Cause: cause}
	assert.ErrorIs(t, wrapped, cause, "errors.Is should find cause via Unwrap chain")
}
