package sriracha

import (
	"errors"
	"fmt"
)

// ErrNotFound is returned by IndexStorage.Get when the key does not exist.
var ErrNotFound = errors.New("not found")

// ErrorCode identifies the class of a Sriracha error.
type ErrorCode int

const (
	CodePolicyMissing        ErrorCode = 1001
	CodeTokenMalformed       ErrorCode = 1002
	CodeFieldSetIncompatible ErrorCode = 1003
	CodeNormalizationFailed  ErrorCode = 1004
	CodeChecksumMismatch     ErrorCode = 1005
	CodeRecordNotFound       ErrorCode = 1006
	CodeIndexCorrupted       ErrorCode = 1007
	CodeAuditViolation       ErrorCode = 1008
	CodeVersionUnsupported   ErrorCode = 1009
	CodeInternalError        ErrorCode = 1010
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

// ErrPolicyMissing returns an error indicating a required policy is absent.
func ErrPolicyMissing() *Error {
	return &Error{Code: CodePolicyMissing, Message: "policy missing"}
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

// ErrRecordNotFound returns an error indicating a record was not found.
func ErrRecordNotFound(id string) *Error {
	return &Error{Code: CodeRecordNotFound, Message: fmt.Sprintf("record not found: %q", id)}
}

// ErrIndexCorrupted returns an error indicating index corruption.
func ErrIndexCorrupted(reason string) *Error {
	return &Error{Code: CodeIndexCorrupted, Message: fmt.Sprintf("index corrupted: %s", reason)}
}

// ErrAuditViolation returns an error indicating an audit integrity violation.
func ErrAuditViolation(reason string) *Error {
	return &Error{Code: CodeAuditViolation, Message: fmt.Sprintf("audit violation: %s", reason)}
}

// ErrVersionUnsupported returns an error indicating an unsupported protocol version.
func ErrVersionUnsupported(version string) *Error {
	return &Error{Code: CodeVersionUnsupported, Message: fmt.Sprintf("version unsupported: %q", version)}
}

// ErrInternalError returns an error indicating an internal error.
func ErrInternalError(reason string) *Error {
	return &Error{Code: CodeInternalError, Message: fmt.Sprintf("internal error: %s", reason)}
}
