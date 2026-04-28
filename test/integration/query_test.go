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
	srirachav1 "go.sriracha.dev/transport/proto/sriracha/v1"
)

// TestIntegration_QueryDeterministic verifies the deterministic Query path
// end-to-end: party A tokenises a record that party B holds (byte-identical
// across all fields) and dispatches a Query; party B's indexer must report
// MATCHED with confidence 1.0 and return the requested raw field values.
func TestIntegration_QueryDeterministic(t *testing.T) {
	t.Parallel()

	c := loadCorpus(t)
	t.Logf("loaded %s", c)

	aID, bID := c.pickIdenticalPair(t)

	h := newTwoPartyHarness(t, c.A, c.B)
	cl := h.dial(t, h.A, h.B)

	tokenizer, err := token.New([]byte(integrationSecret))
	require.NoError(t, err)
	defer tokenizer.Destroy()

	tr, err := tokenizer.TokenizeRecord(c.A[aID], fieldset.DefaultFieldSet())
	require.NoError(t, err)

	policy := h.newPolicy(h.A, h.B, "pol-query-det-"+aID, "integration-test")
	req, err := client.NewQueryRequest(
		tr,
		integrationFieldSetVersion,
		[]string{
			sriracha.FieldNameGiven.String(),
			sriracha.FieldNameFamily.String(),
			sriracha.FieldDateBirth.String(),
		},
		policy,
		nil,
	)
	require.NoError(t, err)

	resp, err := cl.Query(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, resp)
	t.Logf("queried %s↔%s, status=%s confidence=%.3f fields=%d",
		aID, bID, resp.Status, resp.Confidence, len(resp.Fields))

	assert.Equal(t, srirachav1.MatchStatus_MATCH_STATUS_MATCHED, resp.Status,
		"expected MATCHED for byte-identical pair %s↔%s", aID, bID)
	assert.InDelta(t, 1.0, float64(resp.Confidence), 0.001)

	bRaw := c.B[bID]
	got := fieldValueMap(resp.Fields)
	assert.Equal(t, bRaw[sriracha.FieldNameGiven], got[sriracha.FieldNameGiven.String()],
		"returned given_name must equal B's stored value")
	assert.Equal(t, bRaw[sriracha.FieldNameFamily], got[sriracha.FieldNameFamily.String()])
	assert.Equal(t, bRaw[sriracha.FieldDateBirth], got[sriracha.FieldDateBirth.String()])
	assert.Empty(t, resp.NotHeld, "all requested fields are advertised as supported")
	assert.Empty(t, resp.NotFound, "all requested fields are populated in the matching record")
}

// fieldValueMap is a small helper that turns the wire-level field-value
// slice into a path→value map for ergonomic assertions.
func fieldValueMap(fvs []*srirachav1.FieldValue) map[string]string {
	out := make(map[string]string, len(fvs))
	for _, fv := range fvs {
		out[fv.FieldPath] = fv.Value
	}
	return out
}
