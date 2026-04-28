//go:build integration

package integration_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.sriracha.dev/fieldset"
	"go.sriracha.dev/sriracha"
	"go.sriracha.dev/token"
	"go.sriracha.dev/transport/client"
	srirachav1 "go.sriracha.dev/transport/proto/sriracha/v1"
)

// TestIntegration_ConsentRejection verifies that the receiving server's
// consent validator rejects malformed policies end-to-end across mTLS:
// signatures from a key the issuer does not control, expired policies, and
// policies whose target_id does not match the responding institution. Each
// case must surface as a Query error; the audit log records the rejection
// (verified by the harness Close).
func TestIntegration_ConsentRejection(t *testing.T) {
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

	requestedFields := []string{sriracha.FieldNameGiven.String()}

	cases := []struct {
		name     string
		mutate   func(*srirachav1.ConsentPolicy)
		policyID string
	}{
		{
			name:     "wrong signing key",
			policyID: "pol-consent-wrong-key",
			mutate: func(p *srirachav1.ConsentPolicy) {
				_, otherPriv, err := ed25519.GenerateKey(rand.Reader)
				require.NoError(t, err)
				signPolicy(otherPriv, p)
			},
		},
		{
			name:     "expired policy",
			policyID: "pol-consent-expired",
			mutate: func(p *srirachav1.ConsentPolicy) {
				now := time.Now()
				p.IssuedAt = now.Add(-2 * time.Hour).Unix()
				p.ExpiresAt = now.Add(-time.Hour).Unix()
				signPolicy(h.A.signKey, p)
			},
		},
		{
			name:     "wrong target",
			policyID: "pol-consent-wrong-target",
			mutate: func(p *srirachav1.ConsentPolicy) {
				p.TargetId = "org.example.other"
				signPolicy(h.A.signKey, p)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			policy := h.newPolicy(h.A, h.B, tc.policyID, "consent-rejection-test")
			tc.mutate(policy)

			req, err := client.NewQueryRequest(tr, integrationFieldSetVersion, requestedFields, policy, nil)
			require.NoError(t, err)

			_, err = cl.Query(context.Background(), req)
			assert.Error(t, err, "consent rejection must surface as a Query error")
		})
	}
}
