package server

import "context"

type nopAuditLog struct{}

func (nopAuditLog) Append(_ context.Context, _ string, _ map[string]string) error { return nil }
func (nopAuditLog) Verify(_ context.Context) error                                { return nil }
