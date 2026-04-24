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
	future := time.Now().Add(time.Hour)

	cases := []struct {
		name string
		fn   func(*testing.T, *MemoryCache)
	}{
		{"first use succeeds", func(t *testing.T, c *MemoryCache) {
			assert.True(t, c.Claim("pol", future))
		}},
		{"replay rejected", func(t *testing.T, c *MemoryCache) {
			require.True(t, c.Claim("pol", future))
			assert.False(t, c.Claim("pol", future))
		}},
		{"distinct id succeeds", func(t *testing.T, c *MemoryCache) {
			require.True(t, c.Claim("pol-a", future))
			assert.True(t, c.Claim("pol-b", future))
		}},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, New(ctx))
		})
	}
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
