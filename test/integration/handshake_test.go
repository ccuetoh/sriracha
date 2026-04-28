//go:build integration

package integration_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.sriracha.dev/sriracha"
	srirachav1 "go.sriracha.dev/transport/proto/sriracha/v1"
)

// TestIntegration_Handshake verifies that two real institutions can
// complete the GetCapabilities round-trip in both directions over mTLS,
// and that the server's advertised capabilities reflect the configured
// spec/fieldset/match-mode/field-list values.
func TestIntegration_Handshake(t *testing.T) {
	t.Parallel()

	c := loadCorpus(t)
	h := newTwoPartyHarness(t, c.A, c.B)

	t.Run("a dials b", func(t *testing.T) {
		t.Parallel()
		client := h.dial(t, h.A, h.B)
		caps := client.Capabilities()
		require.NotNil(t, caps, "capabilities must be cached after handshake")
		assertCapabilities(t, caps, institutionB)
	})

	t.Run("b dials a", func(t *testing.T) {
		t.Parallel()
		client := h.dial(t, h.B, h.A)
		caps := client.Capabilities()
		require.NotNil(t, caps, "capabilities must be cached after handshake")
		assertCapabilities(t, caps, institutionA)
	})
}

func assertCapabilities(t *testing.T, caps *srirachav1.GetCapabilitiesResponse, _ string) {
	t.Helper()
	assert.Equal(t, integrationSpecVersion, caps.SpecVersion)
	assert.Equal(t, []string{integrationFieldSetVersion}, caps.FieldsetVersions)
	assert.NotEmpty(t, caps.SupportedFields, "server must advertise at least one supported field")
	assert.Contains(t, caps.SupportedFields, sriracha.FieldNameGiven.String())
	assert.Contains(t, caps.SupportedFields, sriracha.FieldDateBirth.String())
	assert.ElementsMatch(t,
		[]srirachav1.MatchMode{
			srirachav1.MatchMode_MATCH_MODE_DETERMINISTIC,
			srirachav1.MatchMode_MATCH_MODE_PROBABILISTIC,
		},
		caps.MatchModes,
	)
}
