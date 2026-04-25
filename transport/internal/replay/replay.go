package replay

import (
	"context"
	"fmt"
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
func New(ctx context.Context) (*MemoryCache, error) {
	b, err := otter.NewBuilder[string, struct{}](100_000)
	if err != nil {
		return nil, fmt.Errorf("replay: build cache: %w", err)
	}
	c, err := b.WithVariableTTL().Build()
	if err != nil {
		return nil, fmt.Errorf("replay: build cache: %w", err)
	}
	mc := &MemoryCache{cache: c}
	go func() {
		<-ctx.Done()
		mc.cache.Close()
	}()
	return mc, nil
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
