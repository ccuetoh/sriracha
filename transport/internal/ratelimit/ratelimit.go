// Package ratelimit enforces per-institution rate limits on the Sriracha
// gRPC server. The Limiter interface keys all decisions on the peer's
// mTLS-derived institution ID; bundled implementations include an
// in-memory token bucket (single instance) and a Redis-backed GCRA
// limiter (multi-instance).
package ratelimit

import (
	"context"
	"errors"
)

// ErrExceeded is returned by Limiter implementations when a request would
// exceed the configured per-institution quota for the requested dimension.
var ErrExceeded = errors.New("rate limit exceeded")

// Limiter enforces per-institution rate limits.
//
// A configured limit of 0 means unlimited for that dimension; the
// corresponding Allow* method returns nil unconditionally.
//
// Implementations must be safe for concurrent use.
type Limiter interface {
	// AllowQuery reports whether one query may be served for institutionID.
	// It returns ErrExceeded if the per-minute quota is exhausted, nil if
	// the request may proceed, or a non-ErrExceeded error if the underlying
	// store failed.
	AllowQuery(ctx context.Context, institutionID string) error

	// AllowBulk reports whether cost bulk records may be served for
	// institutionID. cost must be >= 1; values <= 0 are treated as 1.
	// It returns ErrExceeded if accepting cost would exceed the per-day
	// quota, nil if the request may proceed, or a non-ErrExceeded error
	// if the underlying store failed.
	AllowBulk(ctx context.Context, institutionID string, cost int) error
}

// Noop is a Limiter that always allows. Use it when rate limiting is
// disabled or in tests where the limiter is not under test.
type Noop struct{}

// AllowQuery always returns nil.
func (Noop) AllowQuery(_ context.Context, _ string) error { return nil }

// AllowBulk always returns nil.
func (Noop) AllowBulk(_ context.Context, _ string, _ int) error { return nil }
