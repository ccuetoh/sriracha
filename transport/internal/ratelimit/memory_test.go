package ratelimit

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryAllowQuery(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		queryLimit uint32
		calls      int
		wantErrAt  int // index of first call that should return ErrExceeded; -1 means none
	}{
		{name: "unlimited", queryLimit: 0, calls: 100, wantErrAt: -1},
		{name: "single call within limit", queryLimit: 5, calls: 1, wantErrAt: -1},
		{name: "exactly at burst", queryLimit: 3, calls: 3, wantErrAt: -1},
		{name: "one over burst", queryLimit: 3, calls: 4, wantErrAt: 3},
		{name: "well over burst", queryLimit: 2, calls: 10, wantErrAt: 2},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lim := NewMemory(tc.queryLimit, 0)
			ctx := context.Background()
			for i := 0; i < tc.calls; i++ {
				err := lim.AllowQuery(ctx, "org.test")
				if tc.wantErrAt >= 0 && i >= tc.wantErrAt {
					require.ErrorIs(t, err, ErrExceeded)
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}

func TestMemoryAllowBulk(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		bulkLimit uint32
		costs     []int
		wantErrAt int // index of first call that should return ErrExceeded; -1 means none
	}{
		{name: "unlimited", bulkLimit: 0, costs: []int{1_000_000}, wantErrAt: -1},
		{name: "single small cost within limit", bulkLimit: 100, costs: []int{1}, wantErrAt: -1},
		{name: "exact burst on one batch", bulkLimit: 100, costs: []int{100}, wantErrAt: -1},
		{name: "split across two batches at limit", bulkLimit: 100, costs: []int{50, 50}, wantErrAt: -1},
		{name: "exceeds on second batch", bulkLimit: 100, costs: []int{60, 60}, wantErrAt: 1},
		{name: "single batch over burst", bulkLimit: 10, costs: []int{11}, wantErrAt: 0},
		{name: "non-positive cost coerced to 1", bulkLimit: 1, costs: []int{0, -5}, wantErrAt: 1},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lim := NewMemory(0, tc.bulkLimit)
			ctx := context.Background()
			for i, cost := range tc.costs {
				err := lim.AllowBulk(ctx, "org.test", cost)
				if tc.wantErrAt >= 0 && i >= tc.wantErrAt {
					require.ErrorIs(t, err, ErrExceeded)
				} else {
					require.NoError(t, err, "call %d", i)
				}
			}
		})
	}
}

func TestMemoryPerInstitutionIsolation(t *testing.T) {
	t.Parallel()

	lim := NewMemory(2, 0)
	ctx := context.Background()

	require.NoError(t, lim.AllowQuery(ctx, "org.a"))
	require.NoError(t, lim.AllowQuery(ctx, "org.a"))
	require.ErrorIs(t, lim.AllowQuery(ctx, "org.a"), ErrExceeded)

	// org.b has its own bucket and is unaffected.
	require.NoError(t, lim.AllowQuery(ctx, "org.b"))
	require.NoError(t, lim.AllowQuery(ctx, "org.b"))
	require.ErrorIs(t, lim.AllowQuery(ctx, "org.b"), ErrExceeded)
}

func TestMemoryConcurrentLazyInit(t *testing.T) {
	t.Parallel()

	const goroutines = 64
	lim := NewMemory(uint32(goroutines), 0)
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(goroutines)
	errs := make([]error, goroutines)
	for i := 0; i < goroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			errs[i] = lim.AllowQuery(ctx, "org.race")
		}()
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "goroutine %d", i)
	}
}

func TestNoop(t *testing.T) {
	t.Parallel()

	var l Limiter = Noop{}
	assert.NoError(t, l.AllowQuery(context.Background(), "any"))
	assert.NoError(t, l.AllowBulk(context.Background(), "any", 1_000_000))
	assert.NoError(t, l.AllowBulk(context.Background(), "any", 0))
}

func TestErrExceededIsStable(t *testing.T) {
	t.Parallel()
	// Sanity: callers may errors.Is against the package-level sentinel.
	assert.True(t, errors.Is(ErrExceeded, ErrExceeded))
}

func BenchmarkMemoryLimiter_AllowQuery(b *testing.B) {
	lim := NewMemory(1_000_000_000, 0)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = lim.AllowQuery(ctx, "org.bench")
	}
}

func BenchmarkMemoryLimiter_AllowQuery_Parallel(b *testing.B) {
	lim := NewMemory(1_000_000_000, 0)
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = lim.AllowQuery(ctx, "org.bench")
		}
	})
}

func BenchmarkMemoryLimiter_AllowBulk(b *testing.B) {
	lim := NewMemory(0, 1_000_000_000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = lim.AllowBulk(ctx, "org.bench", 1)
	}
}

func FuzzMemoryLimiter_AllowQuery(f *testing.F) {
	f.Add("")
	f.Add("org.example.a")
	f.Add(string([]byte{0x00, 0xff, 0x7f}))
	f.Add("ünïçødé")

	f.Fuzz(func(t *testing.T, institutionID string) {
		lim := NewMemory(10, 0)
		err := lim.AllowQuery(context.Background(), institutionID)
		if err != nil && !errors.Is(err, ErrExceeded) {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func FuzzMemoryLimiter_AllowBulk(f *testing.F) {
	f.Add("", 1)
	f.Add("org.example.a", 5)
	f.Add(string([]byte{0x00, 0xff, 0x7f}), 0)
	f.Add("ünïçødé", -3)

	f.Fuzz(func(t *testing.T, institutionID string, cost int) {
		lim := NewMemory(0, 100)
		err := lim.AllowBulk(context.Background(), institutionID, cost)
		if err != nil && !errors.Is(err, ErrExceeded) {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}
