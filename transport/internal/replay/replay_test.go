package replay

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClaimExpiredPolicy(t *testing.T) {
	t.Parallel()
	c := newTestCache(t)
	// expiresAt in the past → ttl <= 0 → returns true without caching.
	assert.True(t, c.Claim("pol-already-expired", time.Now().Add(-time.Second)))
}

func newTestCache(t *testing.T) *MemoryCache {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return New(ctx)
}

func TestClaim(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		fn   func(*testing.T, *MemoryCache)
	}{
		{"first use succeeds", func(t *testing.T, c *MemoryCache) {
			assert.True(t, c.Claim("pol", time.Now().Add(time.Hour)))
		}},
		{"replay rejected", func(t *testing.T, c *MemoryCache) {
			require.True(t, c.Claim("pol", time.Now().Add(time.Hour)))
			assert.False(t, c.Claim("pol", time.Now().Add(time.Hour)))
		}},
		{"distinct id succeeds", func(t *testing.T, c *MemoryCache) {
			require.True(t, c.Claim("pol-a", time.Now().Add(time.Hour)))
			assert.True(t, c.Claim("pol-b", time.Now().Add(time.Hour)))
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newTestCache(t))
		})
	}
}

func TestClaimAfterExpiry(t *testing.T) {
	t.Parallel()

	c := newTestCache(t)

	// otter uses a 1-second coarse clock, so the minimum effective TTL is ~1s.
	expiry := time.Now().Add(time.Second)
	require.True(t, c.Claim("pol-old", expiry))

	// After the TTL elapses, the entry is evicted and the same ID can be re-claimed.
	require.Eventually(t, func() bool {
		return c.Claim("pol-old", time.Now().Add(time.Hour))
	}, 5*time.Second, 100*time.Millisecond)
}

func BenchmarkClaim(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := New(ctx)
	expiresAt := time.Now().Add(time.Hour)
	ids := make([]string, b.N)
	for i := range ids {
		ids[i] = "pol-bench-" + strconv.Itoa(i)
	}
	b.ResetTimer()
	for i := range b.N {
		c.Claim(ids[i], expiresAt)
	}
}
