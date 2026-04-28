package sriracha

import "fmt"

// ErrorCode identifies the class of a Sriracha error.
type ErrorCode int

const (
	CodeTokenMalformed       ErrorCode = 1001
	CodeFieldSetIncompatible ErrorCode = 1002
	CodeNormalizationFailed  ErrorCode = 1003
	CodeChecksumMismatch     ErrorCode = 1004
)

// Error is the standard error type returned by this package.
type Error struct {
	Code    ErrorCode
	Message string
	Cause   error
}

func (e *Error) Error() string {
	return fmt.Sprintf("sriracha error %d: %s", e.Code, e.Message)
}

// Unwrap returns the underlying cause, enabling errors.Is and errors.As chaining.
func (e *Error) Unwrap() error { return e.Cause }

// Is reports whether the target error has the same ErrorCode as e.
// This enables errors.Is(err, ErrChecksumMismatch()) comparisons by code.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	return e.Code == t.Code
}

// ErrTokenMalformed returns an error indicating a token is malformed.
func ErrTokenMalformed(field FieldPath) *Error {
	return &Error{Code: CodeTokenMalformed, Message: fmt.Sprintf("token malformed for field %q", field)}
}

// ErrFieldSetIncompatible returns an error indicating FieldSet versions are incompatible.
func ErrFieldSetIncompatible(a, b string) *Error {
	return &Error{Code: CodeFieldSetIncompatible, Message: fmt.Sprintf("fieldset versions incompatible: %q vs %q", a, b)}
}

// ErrNormalizationFailed returns an error indicating normalization failed.
func ErrNormalizationFailed(field FieldPath, reason string) *Error {
	return &Error{Code: CodeNormalizationFailed, Message: fmt.Sprintf("normalization failed for field %q: %s", field, reason)}
}

// ErrChecksumMismatch returns an error indicating a checksum does not match.
func ErrChecksumMismatch() *Error {
	return &Error{Code: CodeChecksumMismatch, Message: "checksum mismatch"}
}
