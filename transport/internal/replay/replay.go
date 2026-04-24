package replay

import (
	"context"
	"sync"
	"time"
)

// Cache is a concurrency-safe replay-prevention store for ConsentPolicy IDs.
// Once a policy ID is claimed it cannot be claimed again until it expires.
type Cache struct {
	mu      sync.Mutex
	entries map[string]time.Time // policyID → expiresAt
}

// New creates a Cache and starts a background pruning goroutine that exits
// when ctx is cancelled. Callers must cancel ctx to avoid goroutine leaks.
func New(ctx context.Context) *Cache {
	c := &Cache{entries: make(map[string]time.Time)}
	go c.pruneLoop(ctx)
	return c
}

// Claim attempts to reserve policyID until expiresAt.
// Returns true on first use, false if already claimed (replay detected).
func (c *Cache) Claim(policyID string, expiresAt time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.entries[policyID]; exists {
		return false
	}

	c.entries[policyID] = expiresAt
	return true
}

func (c *Cache) pruneLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
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

func (c *Cache) prune() {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()

	for id, exp := range c.entries {
		if exp.Before(now) {
			delete(c.entries, id)
		}
	}
}
