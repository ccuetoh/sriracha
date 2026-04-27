package ratelimit

import (
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// redisClientForTest dials REDIS_ADDR (e.g. "localhost:6379") and returns a
// connected client. The test is skipped when REDIS_ADDR is not set so the
// suite stays runnable in environments without Redis.
func redisClientForTest(t testing.TB) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set; skipping Redis integration test")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("Redis not reachable at %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestRedisAllowQuery(t *testing.T) {
	t.Parallel()
	rdb := redisClientForTest(t)

	t.Run("unlimited", func(t *testing.T) {
		t.Parallel()
		l := NewRedis(rdb, 0, 0)
		for i := 0; i < 10; i++ {
			require.NoError(t, l.AllowQuery(context.Background(), "org.unlimited"))
		}
	})

	t.Run("rejects after burst", func(t *testing.T) {
		t.Parallel()
		l := NewRedis(rdb, 3, 0)
		key := "org.query.test." + t.Name()
		_ = rdb.Del(context.Background(), "rate:sriracha:rate_limit:queries_per_minute:"+key).Err()

		for i := 0; i < 3; i++ {
			require.NoError(t, l.AllowQuery(context.Background(), key))
		}
		require.ErrorIs(t, l.AllowQuery(context.Background(), key), ErrExceeded)
	})
}

func TestRedisAllowBulk(t *testing.T) {
	t.Parallel()
	rdb := redisClientForTest(t)

	t.Run("unlimited", func(t *testing.T) {
		t.Parallel()
		l := NewRedis(rdb, 0, 0)
		require.NoError(t, l.AllowBulk(context.Background(), "org.unlimited", 1_000_000))
	})

	t.Run("rejects when cost exceeds remaining", func(t *testing.T) {
		t.Parallel()
		l := NewRedis(rdb, 0, 100)
		key := "org.bulk.test." + t.Name()
		_ = rdb.Del(context.Background(), "rate:sriracha:rate_limit:bulk_records_per_day:"+key).Err()

		require.NoError(t, l.AllowBulk(context.Background(), key, 60))
		require.ErrorIs(t, l.AllowBulk(context.Background(), key, 50), ErrExceeded)
	})

	t.Run("non-positive cost coerced to 1", func(t *testing.T) {
		t.Parallel()
		l := NewRedis(rdb, 0, 1)
		key := "org.bulk.coerce." + t.Name()
		_ = rdb.Del(context.Background(), "rate:sriracha:rate_limit:bulk_records_per_day:"+key).Err()

		require.NoError(t, l.AllowBulk(context.Background(), key, 0))
		require.ErrorIs(t, l.AllowBulk(context.Background(), key, -10), ErrExceeded)
	})
}

func TestRedisAllowStoreError(t *testing.T) {
	t.Parallel()

	// Point at a closed client to force an underlying Redis error path.
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}) // unreachable
	require.NoError(t, rdb.Close())                             // ensure all calls fail

	l := NewRedis(rdb, 5, 5)
	err := l.AllowQuery(context.Background(), "org.err")
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrExceeded)

	err = l.AllowBulk(context.Background(), "org.err", 1)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrExceeded)
}

// TestRedisUnlimitedNoCall ensures the queryLimit==0 / bulkLimit==0 short
// circuit returns nil without ever touching the Redis client. We use a
// closed client so any actual call would error.
func TestRedisUnlimitedNoCall(t *testing.T) {
	t.Parallel()

	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"})
	require.NoError(t, rdb.Close())

	l := NewRedis(rdb, 0, 0)
	require.NoError(t, l.AllowQuery(context.Background(), "org.unlimited"))
	require.NoError(t, l.AllowBulk(context.Background(), "org.unlimited", 1_000_000))
}

func BenchmarkRedisLimiter_AllowQuery(b *testing.B) {
	rdb := redisClientForTest(b)
	l := NewRedis(rdb, 1_000_000, 0)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = l.AllowQuery(ctx, "org.bench")
	}
}

func BenchmarkRedisLimiter_AllowBulk(b *testing.B) {
	rdb := redisClientForTest(b)
	l := NewRedis(rdb, 0, 1_000_000_000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = l.AllowBulk(ctx, "org.bench", 1)
	}
}
