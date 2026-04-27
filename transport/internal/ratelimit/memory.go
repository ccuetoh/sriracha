package ratelimit

import (
	"context"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// MemoryLimiter is an in-memory Limiter backed by golang.org/x/time/rate
// token buckets. One bucket per institution per dimension is created
// lazily on first use and retained for the lifetime of the limiter.
//
// MemoryLimiter is intended for single-instance deployments. For
// multi-instance deployments use a RedisLimiter so quota is shared
// across replicas.
type MemoryLimiter struct {
	queryLimit uint32
	bulkLimit  uint32

	queries sync.Map // institutionID string -> *rate.Limiter
	bulk    sync.Map // institutionID string -> *rate.Limiter
}

// NewMemory returns an in-memory Limiter.
//
// queryLimit is the maximum number of Query RPCs permitted per institution
// per minute (0 = unlimited). bulkLimit is the maximum number of bulk
// records permitted per institution per day (0 = unlimited).
func NewMemory(queryLimit, bulkLimit uint32) *MemoryLimiter {
	return &MemoryLimiter{
		queryLimit: queryLimit,
		bulkLimit:  bulkLimit,
	}
}

// AllowQuery implements Limiter.AllowQuery.
func (m *MemoryLimiter) AllowQuery(_ context.Context, institutionID string) error {
	if m.queryLimit == 0 {
		return nil
	}
	lim := loadOrCreate(&m.queries, institutionID, m.queryLimit, time.Minute)
	if !lim.Allow() {
		return ErrExceeded
	}
	return nil
}

// AllowBulk implements Limiter.AllowBulk.
func (m *MemoryLimiter) AllowBulk(_ context.Context, institutionID string, cost int) error {
	if m.bulkLimit == 0 {
		return nil
	}
	if cost < 1 {
		cost = 1
	}
	lim := loadOrCreate(&m.bulk, institutionID, m.bulkLimit, 24*time.Hour)
	if !lim.AllowN(time.Now(), cost) {
		return ErrExceeded
	}
	return nil
}

// loadOrCreate returns the *rate.Limiter for institutionID, creating one
// with capacity n over the given window if none exists. Concurrent first
// callers may briefly construct redundant limiters; LoadOrStore guarantees
// only one is retained.
func loadOrCreate(m *sync.Map, institutionID string, n uint32, window time.Duration) *rate.Limiter {
	if v, ok := m.Load(institutionID); ok {
		return v.(*rate.Limiter)
	}
	burst := int(n)
	lim := rate.NewLimiter(rate.Every(window/time.Duration(n)), burst)
	actual, _ := m.LoadOrStore(institutionID, lim)
	return actual.(*rate.Limiter)
}
