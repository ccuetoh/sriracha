package server

import "context"

// NopAuditLog is a no-op AuditLog that silently discards all events.
// Useful when audit logging is not required.
type NopAuditLog struct{}

func (NopAuditLog) Append(_ context.Context, _ string, _ map[string]string) error { return nil }
func (NopAuditLog) Verify(_ context.Context) error                                { return nil }
