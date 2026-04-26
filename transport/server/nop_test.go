package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"go.sriracha.dev/sriracha"
)

func TestNopAuditLogAppend(t *testing.T) {
	t.Parallel()
	assert.NoError(t, NopAuditLog{}.Append(context.Background(), sriracha.AuditEvent{}))
}

func TestNopAuditLogVerify(t *testing.T) {
	t.Parallel()
	assert.NoError(t, NopAuditLog{}.Verify(context.Background()))
}
