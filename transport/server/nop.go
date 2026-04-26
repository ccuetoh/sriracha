package server

import (
	"context"

	"go.sriracha.dev/sriracha"
)

// NopAuditLog is a no-op AuditLog that silently discards all events.
// Useful when audit logging is not required.
type NopAuditLog struct{}

func (NopAuditLog) Append(_ context.Context, _ sriracha.AuditEvent) error { return nil }
func (NopAuditLog) Verify(_ context.Context) error                        { return nil }
