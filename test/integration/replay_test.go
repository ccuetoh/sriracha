//go:build integration

package integration_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.sriracha.dev/fieldset"
	"go.sriracha.dev/sriracha"
	"go.sriracha.dev/token"
	"go.sriracha.dev/transport/client"
)

// TestIntegration_ReplayRejection verifies that resubmitting a Query with
// the same ConsentPolicy.policy_id is rejected by the server's replay cache.
// The first call must succeed; the second must fail with the policy already
// claimed.
func TestIntegration_ReplayRejection(t *testing.T) {
	t.Parallel()

	c := loadCorpus(t)
	h := newTwoPartyHarness(t, c.A, c.B)
	cl := h.dial(t, h.A, h.B)

	tokenizer, err := token.New([]byte(integrationSecret))
	require.NoError(t, err)
	defer tokenizer.Destroy()

	aID, _ := c.pickIdenticalPair(t)
	tr, err := tokenizer.TokenizeRecord(c.A[aID], fieldset.DefaultFieldSet())
	require.NoError(t, err)

	policy := h.newPolicy(h.A, h.B, "pol-replay-test", "replay-rejection-test")
	requestedFields := []string{sriracha.FieldNameGiven.String()}

	first, err := client.NewQueryRequest(tr, integrationFieldSetVersion, requestedFields, policy, nil)
	require.NoError(t, err)
	_, err = cl.Query(context.Background(), first)
	require.NoError(t, err, "first use of policy must succeed")

	second, err := client.NewQueryRequest(tr, integrationFieldSetVersion, requestedFields, policy, nil)
	require.NoError(t, err)
	_, err = cl.Query(context.Background(), second)
	assert.Error(t, err, "second use of the same policy_id must be rejected as replay")
}
