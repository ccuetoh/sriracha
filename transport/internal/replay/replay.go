package replay

import (
	"context"
	"sync"
	"time"
)

// Cache is the replay-prevention contract. Implementations must be safe for concurrent use.
type Cache interface {
	Claim(policyID string, expiresAt time.Time) bool
}

// MemoryCache is an in-memory Cache backed by a sync.Map with a background
// pruning goroutine. Create one with New.
type MemoryCache struct {
	entries sync.Map // map[string]time.Time
}

// New creates a MemoryCache and starts a background pruning goroutine that
// exits when ctx is cancelled. Callers must cancel ctx to avoid goroutine leaks.
func New(ctx context.Context) *MemoryCache {
	return NewWithInterval(ctx, time.Minute)
}

// NewWithInterval is like New but uses a custom prune interval. Useful for tests.
func NewWithInterval(ctx context.Context, interval time.Duration) *MemoryCache {
	c := &MemoryCache{}
	go c.pruneLoop(ctx, interval)
	return c
}

// Claim attempts to reserve policyID until expiresAt.
// Returns true on first use, false if already claimed (replay detected).
func (c *MemoryCache) Claim(policyID string, expiresAt time.Time) bool {
	_, loaded := c.entries.LoadOrStore(policyID, expiresAt)
	return !loaded
}

func (c *MemoryCache) pruneLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.prune()
		}
	}
}

func (c *MemoryCache) prune() {
	now := time.Now()
	c.entries.Range(func(key, value any) bool {
		if exp, ok := value.(time.Time); ok && exp.Before(now) {
			c.entries.Delete(key)
		}
		return true
	})
}
