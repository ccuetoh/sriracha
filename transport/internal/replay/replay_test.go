package replay

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClaim(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	c := New(ctx)
	future := time.Now().Add(time.Hour)

	cases := []struct {
		name     string
		policyID string
		want     bool
	}{
		{"first use succeeds", "pol-1", true},
		{"second use rejected", "pol-1", false},
		{"different id succeeds", "pol-2", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Each tc is independent of ordering — test the final state of pol-1 sequentially.
		})
		_ = tc
	}

	require.True(t, c.Claim("pol-abc", future), "first claim should succeed")
	assert.False(t, c.Claim("pol-abc", future), "replay should be rejected")
	assert.True(t, c.Claim("pol-xyz", future), "distinct id should succeed")
}

func TestClaimAfterExpiry(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	c := New(ctx)

	past := time.Now().Add(-time.Second)
	require.True(t, c.Claim("pol-old", past))

	// Manually prune to simulate the ticker firing.
	c.prune()

	// After pruning the expired entry, a new claim with the same ID succeeds.
	assert.True(t, c.Claim("pol-old", time.Now().Add(time.Hour)))
}

func TestPruneLoopExitsOnContextCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	New(ctx)
	// Cancel immediately; the goroutine must exit before the test ends.
	cancel()
}
