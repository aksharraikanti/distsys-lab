package cache

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

var policies = []struct {
	name   string
	policy WritePolicy
}{
	{"WriteInvalidate", WriteInvalidate},
	{"WriteThrough", WriteThrough},
}

// Read-your-writes for a single goroutine, under both policies, for both
// write operations. This replaces Day 1's TestWriteLeavesCachedValueStale,
// which pinned the opposite behavior down until this day fixed it.
func TestReadYourWrites(t *testing.T) {
	for _, p := range policies {
		t.Run(p.name, func(t *testing.T) {
			s := newFakeStore()
			s.Put("k", "old")
			c := New(s, Options{Capacity: 10, WritePolicy: p.policy})
			c.Get("k") // cache "old"

			c.Put("k", "new")
			if got := c.Get("k"); got != "new" {
				t.Fatalf("Get after Put = %q, want new", got)
			}
			c.Append("k", "+more")
			if got := c.Get("k"); got != "new+more" {
				t.Fatalf("Get after Append = %q, want new+more", got)
			}
		})
	}
}

// A key that was never cached must also read its own write, including a key
// whose absence ("") was cached: Day 1's negative-caching staleness.
func TestWriteFixesCachedAbsence(t *testing.T) {
	for _, p := range policies {
		t.Run(p.name, func(t *testing.T) {
			c := New(newFakeStore(), Options{Capacity: 10, WritePolicy: p.policy})
			if got := c.Get("k"); got != "" {
				t.Fatalf("Get(absent) = %q", got)
			}
			c.Put("k", "created")
			if got := c.Get("k"); got != "created" {
				t.Fatalf("Get after creating the key = %q, want created (the cached absence must not survive the write)", got)
			}
		})
	}
}

// The policies differ in exactly one observable way: whether the read right
// after a Put reaches the store.
func TestPoliciesDifferInWhetherTheNextReadMisses(t *testing.T) {
	invalidate := newFakeStore()
	ci := New(invalidate, Options{Capacity: 10, WritePolicy: WriteInvalidate})
	ci.Put("k", "v")
	ci.Get("k")
	if invalidate.getCalls() != 1 {
		t.Fatalf("WriteInvalidate: read after Put made %d store reads, want 1 (a miss)", invalidate.getCalls())
	}

	through := newFakeStore()
	ct := New(through, Options{Capacity: 10, WritePolicy: WriteThrough})
	ct.Put("k", "v")
	ct.Get("k")
	if through.getCalls() != 0 {
		t.Fatalf("WriteThrough: read after Put made %d store reads, want 0 (a hit)", through.getCalls())
	}
}

// Append can't be cached under either policy: the cache can't know old+value.
func TestAppendInvalidatesUnderBothPolicies(t *testing.T) {
	for _, p := range policies {
		t.Run(p.name, func(t *testing.T) {
			s := newFakeStore()
			s.Put("k", "a")
			c := New(s, Options{Capacity: 10, WritePolicy: p.policy})
			c.Get("k")
			before := s.getCalls()
			c.Append("k", "b")
			c.Get("k")
			if s.getCalls() != before+1 {
				t.Fatalf("read after Append must miss (the cache can't compute old+value); store reads went %d -> %d", before, s.getCalls())
			}
			if st := c.Stats(); st.Invalidations != 1 {
				t.Fatalf("Invalidations = %d, want 1", st.Invalidations)
			}
		})
	}
}

// The subtle reason Append doesn't compute cached+value: a stale copy.
func TestAppendDoesNotBuildOnAStaleCachedValue(t *testing.T) {
	s := newFakeStore()
	s.Put("k", "a")
	c := New(s, Options{Capacity: 10, WritePolicy: WriteThrough})
	c.Get("k") // cache "a"

	s.Append("k", "X") // another client appends behind the cache's back: store is "aX"
	c.Append("k", "b") // store is now "aXb"; a cache-computed "ab" would be a value that never existed
	if got := c.Get("k"); got != "aXb" {
		t.Fatalf("Get = %q, want aXb", got)
	}
}

func TestWriteInvalidationKeepsInvariantsAndCountsOnlyRealDrops(t *testing.T) {
	c := New(newFakeStore(), Options{Capacity: 4})
	c.Put("never-cached", "v") // nothing to drop
	if st := c.Stats(); st.Invalidations != 0 {
		t.Fatalf("Invalidations = %d after writing an uncached key, want 0", st.Invalidations)
	}
	c.Get("a")
	c.Put("a", "v")
	c.checkInvariants(t)
	if st := c.Stats(); st.Invalidations != 1 || c.Len() != 0 {
		t.Fatalf("Invalidations = %d, Len = %d; want 1 and 0", st.Invalidations, c.Len())
	}
}

func TestWriteThroughTTLCountsFromWriteStart(t *testing.T) {
	s, clk := newFakeStore(), newFakeClock()
	c := New(s, Options{Capacity: 10, TTL: time.Minute, Clock: clk, WritePolicy: WriteThrough})
	c.Put("k", "v")
	clk.Advance(59 * time.Second)
	if c.Get("k"); s.getCalls() != 0 {
		t.Fatalf("a written-through entry inside its TTL must be a hit")
	}
	clk.Advance(2 * time.Second)
	if c.Get("k"); s.getCalls() != 1 {
		t.Fatalf("a written-through entry must still expire after its TTL")
	}
}

func TestNewRejectsUnknownWritePolicy(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("New with an unknown WritePolicy did not panic")
		}
	}()
	New(newFakeStore(), Options{Capacity: 1, WritePolicy: WritePolicy(99)})
}

// readFirstStore reads the value BEFORE blocking, so a test can freeze a
// reader that has already fetched the old value, do a write, and then let the
// reader finish.
type readFirstStore struct {
	*fakeStore
	entered chan struct{}
	release chan struct{}
}

func (r *readFirstStore) Get(key string) string {
	v := r.fakeStore.Get(key)
	r.entered <- struct{}{}
	<-r.release
	return v
}

// TestStaleFillIsDiscarded is Day 4's known-gap test, flipped. A reader
// fetches "old"; a writer then updates the store AND the cache; the reader
// finally finishes. Its own Get may legitimately return "old" — that read
// overlapped the write — but it must not leave "old" in the cache.
func TestStaleFillIsDiscarded(t *testing.T) {
	for _, p := range policies {
		t.Run(p.name, func(t *testing.T) {
			s := &readFirstStore{fakeStore: newFakeStore(), entered: make(chan struct{}, 4), release: make(chan struct{})}
			s.fakeStore.data["k"] = "old"
			c := New(s, Options{Capacity: 10, WritePolicy: p.policy})

			done := make(chan string)
			go func() { done <- c.Get("k") }()
			<-s.entered // the reader has fetched "old" and is parked

			c.Put("k", "new")
			close(s.release)
			<-done

			if got := c.Get("k"); got != "new" {
				t.Fatalf("Get after the racing fill = %q, want new: the stale fetch must not be cached", got)
			}
			if st := c.Stats(); st.StaleFillsDiscarded != 1 {
				t.Fatalf("StaleFillsDiscarded = %d, want 1", st.StaleFillsDiscarded)
			}
		})
	}
}

// The tradeoff, measured deterministically by counting store reads rather than
// timing anything (a real write costs ~3ms of Raft replication either way,
// which would drown the policy difference).

// Workload A — read-your-write: every key is written and immediately read.
// WriteThrough should turn every one of those reads into a hit.
func TestWriteThroughWinsWhenWrittenKeysAreReadBack(t *testing.T) {
	reads := map[string]int{}
	for _, p := range policies {
		s := newFakeStore()
		c := New(s, Options{Capacity: 1000, WritePolicy: p.policy})
		for i := 0; i < 200; i++ {
			k := fmt.Sprintf("k%d", i)
			c.Put(k, "v")
			c.Get(k)
		}
		reads[p.name] = s.getCalls()
	}
	t.Logf("store reads for 200 write-then-read pairs: %v", reads)
	if reads["WriteThrough"] != 0 || reads["WriteInvalidate"] != 200 {
		t.Fatalf("store reads = %v, want WriteThrough 0 and WriteInvalidate 200", reads)
	}
}

// Workload B — write-mostly to keys nobody reads, plus a small hot read set.
// WriteThrough inserts every written key, so in a bounded cache the never-read
// keys push the hot ones out: the hot reads miss. WriteInvalidate never
// caches a written key, so the hot set stays resident.
func TestWriteThroughPollutesTheCacheWithWriteOnlyKeys(t *testing.T) {
	const capacity = 10
	reads := map[string]int{}
	for _, p := range policies {
		s := newFakeStore()
		c := New(s, Options{Capacity: capacity, WritePolicy: p.policy})
		hot := make([]string, capacity)
		for i := range hot {
			hot[i] = fmt.Sprintf("hot%d", i)
			c.Get(hot[i]) // warm the hot set
		}
		warm := s.getCalls()
		rng := rand.New(rand.NewSource(1))
		for i := 0; i < 500; i++ {
			c.Put(fmt.Sprintf("write-only-%d", i), "v") // never read back
			c.Get(hot[rng.Intn(capacity)])
		}
		reads[p.name] = s.getCalls() - warm
	}
	t.Logf("store reads for 500 (write-only-key, hot-read) rounds: %v", reads)
	if reads["WriteInvalidate"] != 0 {
		t.Fatalf("WriteInvalidate made %d store reads; write-only keys must never displace the hot set", reads["WriteInvalidate"])
	}
	// Not "every round": a miss refills the hot key and evicts a write-only
	// one, so the cache partly recovers each time. What matters is that the
	// hot set now misses at all, hundreds of times, where before it never did.
	if reads["WriteThrough"] < 100 {
		t.Fatalf("WriteThrough made only %d store reads; write-only keys should have displaced the hot set again and again", reads["WriteThrough"])
	}
}

// blockingPutStore parks every Put before it lands, so a test can act in the
// gap between "the write started" and "the store has it".
type blockingPutStore struct {
	*fakeStore
	entered chan struct{}
	release chan struct{}
}

func (b *blockingPutStore) Put(key, value string) {
	b.entered <- struct{}{}
	<-b.release
	b.fakeStore.Put(key, value)
}

// The cache must be updated only AFTER the store has the write. If it were
// invalidated first, a reader arriving in the gap would miss, fetch the old
// value from the store, and re-cache it — and nothing would remove it after
// the write landed. Reading in the gap must see the old value (the write
// hasn't happened yet), and the read after the write must see the new one.
func TestStoreIsWrittenBeforeTheCacheIsTouched(t *testing.T) {
	for _, p := range policies {
		t.Run(p.name, func(t *testing.T) {
			s := &blockingPutStore{fakeStore: newFakeStore(), entered: make(chan struct{}, 1), release: make(chan struct{})}
			s.fakeStore.data["k"] = "old"
			c := New(s, Options{Capacity: 10, WritePolicy: p.policy})
			c.Get("k") // cache "old"

			done := make(chan struct{})
			go func() { c.Put("k", "new"); close(done) }()
			<-s.entered // the Put has started but the store doesn't have it yet

			if got := c.Get("k"); got != "old" {
				t.Fatalf("Get during an in-flight Put = %q, want old (the write has not landed)", got)
			}
			close(s.release)
			<-done

			if got := c.Get("k"); got != "new" {
				t.Fatalf("Get after the Put returned = %q, want new (a reader in the gap must not leave the old value cached)", got)
			}
		})
	}
}
