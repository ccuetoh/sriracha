package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNopAuditLogVerify(t *testing.T) {
	t.Parallel()
	assert.NoError(t, NopAuditLog{}.Verify(context.Background()))
}
