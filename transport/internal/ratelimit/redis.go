package ratelimit

import (
	"context"
	"fmt"
	"time"

	redis_rate "github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"
)

// redisLimiter is a Limiter backed by Redis using the GCRA algorithm
// implemented by redis_rate. Quota state is shared across all clients
// pointed at the same Redis instance, which is the intended deployment
// shape for multi-replica Sriracha servers.
//
// The caller owns the *redis.Client lifecycle.
type redisLimiter struct {
	queryLimit uint32
	bulkLimit  uint32
	limiter    *redis_rate.Limiter
}

// NewRedis returns a Redis-backed Limiter.
//
// rdb must be a connected *redis.Client. queryLimit is the maximum number
// of Query RPCs permitted per institution per minute (0 = unlimited).
// bulkLimit is the maximum number of bulk records permitted per
// institution per day (0 = unlimited).
func NewRedis(rdb *redis.Client, queryLimit, bulkLimit uint32) Limiter {
	return &redisLimiter{
		queryLimit: queryLimit,
		bulkLimit:  bulkLimit,
		limiter:    redis_rate.NewLimiter(rdb),
	}
}

func (r *redisLimiter) AllowQuery(ctx context.Context, institutionID string) error {
	if r.queryLimit == 0 {
		return nil
	}
	key := fmt.Sprintf("sriracha:rate_limit:queries_per_minute:%s", institutionID)
	limit := redis_rate.Limit{
		Rate:   int(r.queryLimit),
		Burst:  int(r.queryLimit),
		Period: time.Minute,
	}
	res, err := r.limiter.Allow(ctx, key, limit)
	if err != nil {
		return fmt.Errorf("ratelimit: redis allow query: %w", err)
	}
	if res.Allowed < 1 {
		return ErrExceeded
	}
	return nil
}

func (r *redisLimiter) AllowBulk(ctx context.Context, institutionID string, cost int) error {
	if r.bulkLimit == 0 {
		return nil
	}
	if cost < 1 {
		cost = 1
	}
	key := fmt.Sprintf("sriracha:rate_limit:bulk_records_per_day:%s", institutionID)
	limit := redis_rate.Limit{
		Rate:   int(r.bulkLimit),
		Burst:  int(r.bulkLimit),
		Period: 24 * time.Hour,
	}
	res, err := r.limiter.AllowN(ctx, key, limit, cost)
	if err != nil {
		return fmt.Errorf("ratelimit: redis allow bulk: %w", err)
	}
	if res.Allowed < cost {
		return ErrExceeded
	}
	return nil
}
