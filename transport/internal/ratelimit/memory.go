package ratelimit

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// memoryLimiter is an in-memory Limiter backed by golang.org/x/time/rate
// token buckets. One bucket per institution per dimension is created
// lazily on first use and retained for the lifetime of the limiter.
//
// It is intended for single-instance deployments. For multi-instance
// deployments use the limiter returned by NewRedis so quota is shared
// across replicas.
type memoryLimiter struct {
	queryLimit uint32
	bulkLimit  uint32

	queries limiterMap
	bulk    limiterMap
}

// limiterMap is a goroutine-safe map of institution ID to *rate.Limiter.
// It is the typed equivalent of a sync.Map specialised for this package
// and avoids unchecked type assertions at the call site.
type limiterMap struct {
	mu sync.Mutex
	m  map[string]*rate.Limiter
}

// loadOrCreate returns the *rate.Limiter for institutionID, creating
// one with capacity n over the given window if none exists. The single
// lock keeps the contention path cheap relative to the rate.Limiter's
// own atomic work that follows.
func (lm *limiterMap) loadOrCreate(institutionID string, n uint32, window time.Duration) *rate.Limiter {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if lim, ok := lm.m[institutionID]; ok {
		return lim
	}
	if lm.m == nil {
		lm.m = make(map[string]*rate.Limiter)
	}
	lim := rate.NewLimiter(rate.Every(window/time.Duration(n)), int(n))
	lm.m[institutionID] = lim
	return lim
}

// NewMemory returns an in-memory Limiter.
//
// queryLimit is the maximum number of Query RPCs permitted per institution
// per minute (0 = unlimited). bulkLimit is the maximum number of bulk
// records permitted per institution per day (0 = unlimited).
func NewMemory(queryLimit, bulkLimit uint32) Limiter {
	return &memoryLimiter{
		queryLimit: queryLimit,
		bulkLimit:  bulkLimit,
	}
}

func (m *memoryLimiter) AllowQuery(_ context.Context, institutionID string) error {
	if m.queryLimit == 0 {
		return nil
	}
	if !m.queries.loadOrCreate(institutionID, m.queryLimit, time.Minute).Allow() {
		return ErrExceeded
	}
	return nil
}

func (m *memoryLimiter) AllowBulk(_ context.Context, institutionID string, cost int) error {
	if m.bulkLimit == 0 {
		return nil
	}
	if cost < 1 {
		cost = 1
	}
	if !m.bulk.loadOrCreate(institutionID, m.bulkLimit, 24*time.Hour).AllowN(time.Now(), cost) {
		return ErrExceeded
	}
	return nil
}
