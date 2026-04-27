package ratelimit

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// startMiniredis spins up an in-process Redis server so the GCRA Lua
// script in redis_rate can execute without an external dependency.
func startMiniredis(t testing.TB) *redis.Client {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestRedisAllowQuery(t *testing.T) {
	t.Parallel()
	rdb := startMiniredis(t)

	t.Run("rejects after burst", func(t *testing.T) {
		t.Parallel()
		l := NewRedis(rdb, 3, 0)
		ctx := context.Background()

		for i := 0; i < 3; i++ {
			require.NoError(t, l.AllowQuery(ctx, "org.query.burst"))
		}
		require.ErrorIs(t, l.AllowQuery(ctx, "org.query.burst"), ErrExceeded)
	})

	t.Run("isolated per institution", func(t *testing.T) {
		t.Parallel()
		l := NewRedis(rdb, 1, 0)
		ctx := context.Background()

		require.NoError(t, l.AllowQuery(ctx, "org.query.iso.a"))
		require.ErrorIs(t, l.AllowQuery(ctx, "org.query.iso.a"), ErrExceeded)

		// Different institution has its own bucket.
		require.NoError(t, l.AllowQuery(ctx, "org.query.iso.b"))
	})
}

func TestRedisAllowBulk(t *testing.T) {
	t.Parallel()
	rdb := startMiniredis(t)

	t.Run("rejects when cost exceeds remaining", func(t *testing.T) {
		t.Parallel()
		l := NewRedis(rdb, 0, 100)
		ctx := context.Background()

		require.NoError(t, l.AllowBulk(ctx, "org.bulk.cost", 60))
		require.ErrorIs(t, l.AllowBulk(ctx, "org.bulk.cost", 50), ErrExceeded)
	})

	t.Run("non-positive cost coerced to 1", func(t *testing.T) {
		t.Parallel()
		l := NewRedis(rdb, 0, 1)
		ctx := context.Background()

		require.NoError(t, l.AllowBulk(ctx, "org.bulk.coerce", 0))
		require.ErrorIs(t, l.AllowBulk(ctx, "org.bulk.coerce", -10), ErrExceeded)
	})

	t.Run("split across batches at limit", func(t *testing.T) {
		t.Parallel()
		l := NewRedis(rdb, 0, 100)
		ctx := context.Background()

		require.NoError(t, l.AllowBulk(ctx, "org.bulk.split", 50))
		require.NoError(t, l.AllowBulk(ctx, "org.bulk.split", 50))
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
	rdb := startMiniredis(b)
	l := NewRedis(rdb, 1_000_000, 0)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = l.AllowQuery(ctx, "org.bench")
	}
}

func BenchmarkRedisLimiter_AllowBulk(b *testing.B) {
	rdb := startMiniredis(b)
	l := NewRedis(rdb, 0, 1_000_000_000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = l.AllowBulk(ctx, "org.bench", 1)
	}
}
