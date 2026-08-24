package sriracha

import (
	"errors"
	"fmt"
)

// Sentinel errors for schema, record, and configuration problems. Every
// error the module returns for these conditions wraps one of them, so
// callers can branch with errors.Is instead of matching message text.
//
// Errors that carry a field path are wrapped in a FieldError, which keeps
// the sentinel reachable through errors.Is and the path recoverable through
// errors.As.
var (
	// ErrInvalidFieldPath reports a path that is not <org>::<namespace>::<name>
	// with all three components non-empty, or a FieldSpec with no path.
	ErrInvalidFieldPath = errors.New("invalid field path")

	// ErrUnknownField reports a path that does not appear in the FieldSet.
	ErrUnknownField = errors.New("unknown field")

	// ErrDuplicateField reports a path that appears more than once in a
	// FieldSet.
	ErrDuplicateField = errors.New("duplicate field path")

	// ErrRequiredFieldMissing reports a field marked Required that the
	// record does not carry.
	ErrRequiredFieldMissing = errors.New("required field missing")

	// ErrEmptyValue reports a value that is empty, or that normalizes to
	// the empty string, where a value is required.
	ErrEmptyValue = errors.New("value is empty")

	// ErrInvalidWeight reports a FieldSpec weight that is negative, NaN, or
	// infinite.
	ErrInvalidWeight = errors.New("invalid field weight")

	// ErrMissingVersion reports a FieldSet with an empty Version.
	ErrMissingVersion = errors.New("field set version must not be empty")

	// ErrInvalidConfig reports a ProbabilisticConfig that would crash or
	// produce degenerate filters at tokenization time.
	ErrInvalidConfig = errors.New("invalid probabilistic config")
)

// FieldError attaches a field path to the error that field produced. It is
// the leaf type of the joined errors returned by record and schema
// validation: errors.Is finds the underlying sentinel, errors.As recovers
// the path.
//
// Path holds the raw text of the offending key even when that text is not a
// valid field path, so the caller can report which input was rejected. In
// that case only Path.String() is meaningful; the component accessors are
// empty.
type FieldError struct {
	Path FieldPath
	Err  error
}

// Error implements error.
func (e FieldError) Error() string {
	return fmt.Sprintf("field %q: %v", e.Path, e.Err)
}

// Unwrap returns the underlying error so errors.Is reaches the sentinel.
func (e FieldError) Unwrap() error { return e.Err }
