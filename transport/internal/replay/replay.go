package replay

import (
	"context"
	"sync"
	"time"

	"github.com/maypok86/otter"
)

// Cache is the replay-prevention contract. Implementations must be safe for concurrent use.
type Cache interface {
	Claim(policyID string, expiresAt time.Time) bool
}

// MemoryCache is an in-memory Cache backed by otter with automatic TTL eviction.
// Create one with New.
type MemoryCache struct {
	mu    sync.Mutex
	cache otter.CacheWithVariableTTL[string, struct{}]
}

// New creates a MemoryCache backed by an otter cache. The cache is closed when ctx is cancelled.
func New(ctx context.Context) *MemoryCache {
	//nolint:errcheck // Build only fails when capacity <= 0; MustBuilder guarantees capacity is valid.
	c, _ := otter.MustBuilder[string, struct{}](100_000).WithVariableTTL().Build()
	mc := &MemoryCache{cache: c}
	go func() {
		<-ctx.Done()
		mc.cache.Close()
	}()
	return mc
}

// Claim attempts to reserve policyID until expiresAt.
// Returns true on first use, false if already claimed (replay detected).
func (c *MemoryCache) Claim(policyID string, expiresAt time.Time) bool {
	ttl := time.Until(expiresAt)
	if ttl <= 0 {
		return true
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Has returns false for expired entries, enabling re-claim after expiry.
	if c.cache.Has(policyID) {
		return false
	}
	return c.cache.Set(policyID, struct{}{}, ttl)
}
