package cache

import (
	"sync"
	"testing"
	"time"
)

// fakeClock is a Clock the test advances by hand: expiry becomes instant and
// exact instead of depending on real sleeps.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_000_000, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func ttlCache(s Store, ttl time.Duration, clk Clock) *Cache {
	return New(s, Options{Capacity: 10, TTL: ttl, Clock: clk})
}

func TestEntryServedBeforeTTLAndRefetchedAfter(t *testing.T) {
	s, clk := newFakeStore(), newFakeClock()
	s.Put("k", "v1")
	c := ttlCache(s, time.Minute, clk)

	c.Get("k")
	clk.Advance(59 * time.Second)
	c.Get("k")
	if s.getCalls() != 1 {
		t.Fatalf("store Get called %d times before the TTL, want 1", s.getCalls())
	}

	clk.Advance(2 * time.Second) // now 61s after the fill
	c.Get("k")
	if s.getCalls() != 2 {
		t.Fatalf("store Get called %d times after the TTL, want 2 (expired entry must be refetched)", s.getCalls())
	}
	if st := c.Stats(); st.Expirations != 1 || st.Hits != 1 || st.Misses != 2 {
		t.Fatalf("Stats = %+v, want 1 expiration, 1 hit, 2 misses (an expired entry is a miss, not a hit)", st)
	}
}

// A TTL of d means "served for less than d": at exactly the deadline the
// entry is already expired.
func TestExpiryBoundary(t *testing.T) {
	s, clk := newFakeStore(), newFakeClock()
	c := ttlCache(s, time.Minute, clk)
	c.Get("k")

	clk.Advance(time.Minute - time.Nanosecond)
	c.Get("k")
	if s.getCalls() != 1 {
		t.Fatalf("one nanosecond before the deadline must still be a hit")
	}
	clk.Advance(time.Nanosecond) // exactly the deadline
	c.Get("k")
	if s.getCalls() != 2 {
		t.Fatalf("exactly at the deadline must be expired")
	}
}

// Refreshing on read would let a hot key stay cached — and stale — forever.
func TestHitDoesNotExtendTTL(t *testing.T) {
	s, clk := newFakeStore(), newFakeClock()
	c := ttlCache(s, time.Minute, clk)
	c.Get("k") // fill at t=0
	for i := 0; i < 5; i++ {
		clk.Advance(11 * time.Second)
		c.Get("k") // hits at 11s..55s
	}
	if s.getCalls() != 1 {
		t.Fatalf("hits inside the TTL must not reach the store, got %d calls", s.getCalls())
	}
	clk.Advance(6 * time.Second) // 61s after the ORIGINAL fill
	c.Get("k")
	if s.getCalls() != 2 {
		t.Fatalf("a key read every 11s must still expire 60s after its fill; hits must not extend the deadline")
	}
}

// The payoff: TTL bounds the staleness Day 1's write-through-only gap and
// other clients' writes cause. (Day 4 closes the gap for this cache's own
// writes; TTL is what covers everyone else's.)
func TestTTLBoundsStaleness(t *testing.T) {
	s, clk := newFakeStore(), newFakeClock()
	s.Put("k", "old")
	c := ttlCache(s, time.Minute, clk)
	c.Get("k")

	s.Put("k", "new") // another client changes it behind the cache's back
	if got := c.Get("k"); got != "old" {
		t.Fatalf("within the TTL the cache serves the stale value; got %q", got)
	}
	clk.Advance(time.Minute)
	if got := c.Get("k"); got != "new" {
		t.Fatalf("after the TTL the change must be visible; got %q", got)
	}
}

func TestZeroTTLNeverExpires(t *testing.T) {
	s, clk := newFakeStore(), newFakeClock()
	c := ttlCache(s, 0, clk)
	c.Get("k")
	clk.Advance(100 * 365 * 24 * time.Hour)
	c.Get("k")
	if s.getCalls() != 1 {
		t.Fatalf("TTL 0 must mean no expiry, but the entry was refetched")
	}
}

func TestExpiredEntryIsRemovedAndInvariantsHold(t *testing.T) {
	s, clk := newFakeStore(), newFakeClock()
	c := ttlCache(s, time.Minute, clk)
	c.Get("a")
	c.Get("b")
	clk.Advance(2 * time.Minute)
	c.Get("a") // a expires and is refilled
	c.checkInvariants(t)
	if c.Len() != 2 {
		t.Fatalf("Len = %d, want 2 (a refilled, b still resident but expired)", c.Len())
	}
	c.Get("b")
	c.checkInvariants(t)
	if st := c.Stats(); st.Expirations != 2 {
		t.Fatalf("Expirations = %d, want 2", st.Expirations)
	}
}

// slowStore advances the fake clock while a Get is in flight, simulating a
// slow backing store.
type slowStore struct {
	*fakeStore
	clk   *fakeClock
	delay time.Duration
}

func (s *slowStore) Get(key string) string {
	v := s.fakeStore.Get(key)
	s.clk.Advance(s.delay)
	return v
}

// The deadline counts from when the fetch STARTED: a value returned by a slow
// fetch was only known to be current at some point during it.
func TestDeadlineCountsFromFetchStart(t *testing.T) {
	clk := newFakeClock()
	s := &slowStore{fakeStore: newFakeStore(), clk: clk, delay: 40 * time.Second}
	c := ttlCache(s, time.Minute, clk)

	c.Get("k") // fetch starts at t=0, returns at t=40s

	clk.Advance(19 * time.Second) // t=59s: 19s after the fetch ended, 59s after it started
	c.Get("k")
	if s.getCalls() != 1 {
		t.Fatalf("t=59s is inside the 60s TTL counted from fetch start; must be a hit")
	}
	clk.Advance(2 * time.Second) // t=61s: only 21s after the fetch ended
	c.Get("k")
	if s.getCalls() != 2 {
		t.Fatalf("t=61s must be expired: the TTL counts from fetch start, not fetch end (an end-based deadline would say 21s old)")
	}
}

func TestNewRejectsNegativeTTL(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("New with a negative TTL did not panic")
		}
	}()
	New(newFakeStore(), Options{Capacity: 1, TTL: -time.Second})
}

// The default (nil Clock) path really uses the wall clock. Only the
// "expires after real time passes" direction is asserted, because it can't
// spuriously fail under load — sleeping never undershoots — unlike asserting
// an entry is still fresh within a tiny TTL.
func TestRealClockIsTheDefault(t *testing.T) {
	s := newFakeStore()
	c := New(s, Options{Capacity: 4, TTL: 20 * time.Millisecond})
	c.Get("k")
	time.Sleep(60 * time.Millisecond)
	c.Get("k")
	if s.getCalls() != 2 {
		t.Fatalf("with the default clock, an entry older than its TTL must be refetched")
	}
}
