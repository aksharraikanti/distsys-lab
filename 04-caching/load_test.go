package cache

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"
)

// zipfKeys returns a generator of key names drawn from a Zipf distribution
// over n keys: a few keys are read very often and most are read rarely, which
// is the shape real read traffic has and the only shape a cache is worth
// having for. Seeded, so runs are repeatable.
func zipfKeys(seed int64, n int) func() string {
	rng := rand.New(rand.NewSource(seed))
	z := rand.NewZipf(rng, 1.2, 1, uint64(n-1))
	return func() string { return fmt.Sprintf("key-%d", z.Uint64()) }
}

// TestHitRateGrowsWithCapacity: the classic cache curve, measured. Same
// seeded Zipf read sequence at each capacity. LRU is a "stack algorithm" — the
// set of entries a size-k cache holds is always a subset of what a size-(k+1)
// cache holds — so the hit count can only grow with capacity, which makes
// monotonicity a guarantee to assert rather than a hope.
func TestHitRateGrowsWithCapacity(t *testing.T) {
	const keys, reads = 200, 20000
	prev := -1.0
	for _, capacity := range []int{2, 5, 10, 20, 50, 100, 200} {
		s := newFakeStore()
		c := New(s, Options{Capacity: capacity})
		next := zipfKeys(1, keys)
		for i := 0; i < reads; i++ {
			c.Get(next())
		}
		st := c.Stats()
		rate := float64(st.Hits) / float64(st.Hits+st.Misses)
		t.Logf("capacity %3d of %d keys (%3.0f%%): hit rate %5.1f%%, evictions %d",
			capacity, keys, 100*float64(capacity)/keys, 100*rate, st.Evictions)
		if rate < prev {
			t.Fatalf("hit rate fell from %.3f to %.3f as capacity grew to %d; LRU hit rate cannot decrease", prev, rate, capacity)
		}
		prev = rate
	}
	if prev < 0.95 {
		t.Fatalf("a cache holding every key should hit ~always after warm-up, got %.3f", prev)
	}
}

type latencies []time.Duration

func (l latencies) pct(p float64) time.Duration {
	s := append(latencies(nil), l...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[int(p*float64(len(s)-1))]
}

func (l latencies) mean() time.Duration {
	var sum time.Duration
	for _, d := range l {
		sum += d
	}
	return sum / time.Duration(len(l))
}

// runReads has `workers` goroutines each do `perWorker` Zipf reads through
// get, timing every read.
func runReads(workers, perWorker, keys int, get func(string) string) latencies {
	var mu sync.Mutex
	var all latencies
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			next := zipfKeys(int64(100+w), keys)
			local := make(latencies, 0, perWorker)
			for i := 0; i < perWorker; i++ {
				k := next()
				t0 := time.Now()
				get(k)
				local = append(local, time.Since(t0))
			}
			mu.Lock()
			all = append(all, local...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	return all
}

// TestCachedVsUncachedReadLatency: the stack's headline number. A Zipf read
// workload over the real Raft + TCP + pool stack, uncached and then behind the
// cache. Medians are compared rather than means: the cached median is a
// memory hit and the uncached one is a network round trip, so the gap is
// orders of magnitude and survives CPU noise, where a comparison of means
// would depend on how the tail happened to fall.
func TestCachedVsUncachedReadLatency(t *testing.T) {
	client, cleanup := pooledStack(t)
	defer cleanup()

	const keys, workers, perWorker = 100, 4, 1500
	for i := 0; i < keys; i++ {
		client.Put(fmt.Sprintf("key-%d", i), "value")
	}

	uncached := runReads(workers, perWorker, keys, client.Get)

	c := New(client, Options{Capacity: 25})
	cached := runReads(workers, perWorker, keys, c.Get)
	st := c.Stats()
	rate := float64(st.Hits) / float64(st.Hits+st.Misses)

	t.Logf("uncached reads: p50 %v  p99 %v  mean %v", uncached.pct(0.50), uncached.pct(0.99), uncached.mean())
	t.Logf("cached   reads: p50 %v  p99 %v  mean %v", cached.pct(0.50), cached.pct(0.99), cached.mean())
	t.Logf("cache (25 of %d keys): hit rate %.1f%%, coalesced %d, evictions %d", keys, 100*rate, st.Coalesced, st.Evictions)

	if rate < 0.5 {
		t.Fatalf("hit rate %.2f under a Zipf workload with a 25%% cache; expected well over half", rate)
	}
	if cached.pct(0.50)*10 >= uncached.pct(0.50) {
		t.Fatalf("cached p50 %v is not at least 10x faster than uncached p50 %v", cached.pct(0.50), uncached.pct(0.50))
	}
	if cached.mean() >= uncached.mean() {
		t.Fatalf("cached mean %v is not faster than uncached mean %v", cached.mean(), uncached.mean())
	}
}
