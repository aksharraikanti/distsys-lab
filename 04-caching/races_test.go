package cache

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// waitUntil polls cond until true or a generous deadline. It is used only to
// wait for goroutines to REACH a state the test then acts on (e.g. "all the
// joiners are now waiting"), never to decide pass/fail.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// --- Stampede -------------------------------------------------------------

// Twenty callers miss on one key while the fetch is parked: the Store must be
// asked once, and every caller must get the answer.
func TestConcurrentMissesAreCoalescedIntoOneFetch(t *testing.T) {
	const callers = 20
	g := &gatedStore{fakeStore: newFakeStore(), entered: make(chan struct{}, callers), release: make(chan struct{})}
	g.fakeStore.data["hot"] = "v"
	c := New(g, Options{Capacity: 10})

	results := make(chan string, callers)
	for i := 0; i < callers; i++ {
		go func() { results <- c.Get("hot") }()
	}
	waitUntil(t, "19 joiners", func() bool { return c.Stats().Coalesced == callers-1 })
	<-g.entered // exactly one caller is inside the store
	close(g.release)

	for i := 0; i < callers; i++ {
		if got := <-results; got != "v" {
			t.Fatalf("a caller got %q, want v", got)
		}
	}
	if got := g.fakeStore.getCalls(); got != 1 {
		t.Fatalf("store Get called %d times for %d concurrent misses on one key, want 1", got, callers)
	}
	if st := c.Stats(); st.Misses != callers || st.Coalesced != callers-1 {
		t.Fatalf("Stats = %+v, want %d misses and %d coalesced", st, callers, callers-1)
	}
	c.checkInvariants(t)
}

// Coalescing is per key: different keys must not wait on each other.
func TestDifferentKeysAreNotCoalesced(t *testing.T) {
	const keys, per = 3, 5
	g := &gatedStore{fakeStore: newFakeStore(), entered: make(chan struct{}, keys*per), release: make(chan struct{})}
	c := New(g, Options{Capacity: 10})

	var wg sync.WaitGroup
	for k := 0; k < keys; k++ {
		for i := 0; i < per; i++ {
			wg.Add(1)
			go func(k int) { defer wg.Done(); c.Get(fmt.Sprintf("k%d", k)) }(k)
		}
	}
	waitUntil(t, "12 joiners", func() bool { return c.Stats().Coalesced == keys*(per-1) })
	for k := 0; k < keys; k++ {
		<-g.entered // one leader per key
	}
	close(g.release)
	wg.Wait()
	if got := g.fakeStore.getCalls(); got != keys {
		t.Fatalf("store Get called %d times for %d distinct keys, want %d", got, keys, keys)
	}
}

// panicOnceStore parks its first Get and then panics; later Gets succeed.
type panicOnceStore struct {
	*fakeStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *panicOnceStore) Get(key string) string {
	first := false
	p.once.Do(func() { first = true })
	if first {
		p.entered <- struct{}{}
		<-p.release
		panic("store failure")
	}
	return p.fakeStore.Get(key)
}

// A leader whose fetch panics must not strand the callers waiting on it.
func TestJoinersRetryIfTheLeaderPanics(t *testing.T) {
	p := &panicOnceStore{fakeStore: newFakeStore(), entered: make(chan struct{}, 1), release: make(chan struct{})}
	p.fakeStore.data["k"] = "v"
	c := New(p, Options{Capacity: 10})

	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		defer func() { _ = recover() }() // the leader's own caller sees the panic
		c.Get("k")
	}()
	<-p.entered

	joiner := make(chan string, 1)
	go func() { joiner <- c.Get("k") }()
	waitUntil(t, "the joiner", func() bool { return c.Stats().Coalesced == 1 })

	close(p.release) // the leader panics
	<-leaderDone
	select {
	case got := <-joiner:
		if got != "v" {
			t.Fatalf("joiner got %q after the leader panicked, want v (it should retry itself)", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("joiner is stuck: a panicking leader must release its waiters")
	}
	c.mu.Lock()
	n := len(c.inflight)
	c.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d flights left registered after everything finished", n)
	}
}

// --- Stale fill -----------------------------------------------------------

// oneShotGateStore parks only its FIRST Get (after reading the value), so a
// later Get in the same test passes straight through.
type oneShotGateStore struct {
	*fakeStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (o *oneShotGateStore) Get(key string) string {
	v := o.fakeStore.Get(key)
	block := false
	o.once.Do(func() { block = true })
	if block {
		o.entered <- struct{}{}
		<-o.release
	}
	return v
}

// The subtle half of the stale-fill fix. The writer's own Get, issued right
// after its Put, must not join the still-running pre-write fetch and be handed
// the old value: that would break read-your-writes for the very goroutine that
// wrote. Marking the flight stale is not enough; it must also be detached.
//
// Only the WriteInvalidate subtest can catch a missing detach: under WriteThrough
// the Put itself populates the cache, so the writer's next Get is a plain hit
// and never reaches the in-flight logic at all.
func TestWriterDoesNotJoinAStaleFlight(t *testing.T) {
	for _, p := range policies {
		t.Run(p.name, func(t *testing.T) {
			s := &oneShotGateStore{fakeStore: newFakeStore(), entered: make(chan struct{}, 1), release: make(chan struct{})}
			s.fakeStore.data["k"] = "old"
			c := New(s, Options{Capacity: 10, WritePolicy: p.policy})

			slow := make(chan string, 1)
			go func() { slow <- c.Get("k") }()
			<-s.entered // a pre-write fetch holding "old" is in flight

			c.Put("k", "new")
			// Run the Get in a goroutine with a deadline: if it wrongly joins
			// the stale flight it BLOCKS until that fetch is released, and a
			// bug should fail this test in seconds rather than hang it.
			mine := make(chan string, 1)
			go func() { mine <- c.Get("k") }()
			select {
			case got := <-mine:
				if got != "new" {
					t.Fatalf("Get right after Put = %q, want new — it joined the stale in-flight fetch", got)
				}
			case <-time.After(5 * time.Second):
				close(s.release) // let everything unwind
				t.Fatal("Get right after Put blocked behind the stale in-flight fetch: the flight was marked stale but not detached")
			}

			close(s.release)
			<-slow // the slow reader finishes with its (legal) old answer
			if got := c.Get("k"); got != "new" {
				t.Fatalf("Get after the slow fetch finished = %q, want new", got)
			}
			if st := c.Stats(); st.StaleFillsDiscarded != 1 {
				t.Fatalf("StaleFillsDiscarded = %d, want 1", st.StaleFillsDiscarded)
			}
		})
	}
}

// --- Write reordering ------------------------------------------------------

// landThenParkStore lands the write "v1" in the store and then parks BEFORE
// returning — the window in which a second writer can overtake the first.
type landThenParkStore struct {
	*fakeStore
	landed  chan struct{}
	release chan struct{}
}

func (l *landThenParkStore) Put(key, value string) {
	l.fakeStore.Put(key, value)
	if value == "v1" {
		l.landed <- struct{}{}
		<-l.release
	}
}

// Writer 1's store write lands first but its cache update is delayed; writer 2
// then writes v2. Unserialized, the cache updates land as v2 then v1 while the
// store ended at v2: a WriteThrough cache permanently wrong. Serializing writes
// keeps cache order equal to store order.
//
// The short sleep only gives an UNSERIALIZED implementation time to show the
// bug (writer 2 finishing while writer 1 is parked); it can never make a
// correct implementation fail.
func TestConcurrentWritersCannotReorderCacheUpdates(t *testing.T) {
	s := &landThenParkStore{fakeStore: newFakeStore(), landed: make(chan struct{}, 1), release: make(chan struct{})}
	c := New(s, Options{Capacity: 10, WritePolicy: WriteThrough})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); c.Put("k", "v1") }()
	<-s.landed // v1 is in the store; writer 1 hasn't updated the cache yet
	go func() { defer wg.Done(); c.Put("k", "v2") }()
	time.Sleep(50 * time.Millisecond)
	close(s.release)
	wg.Wait()

	storeVal := s.fakeStore.data["k"]
	if got := c.Get("k"); got != storeVal {
		t.Fatalf("cache serves %q but the store holds %q: writes were applied to the cache in a different order than to the store", got, storeVal)
	}
}

// --- Whole-system property -------------------------------------------------

// After a burst of concurrent reads and writes on a few hot keys, every entry
// the cache still holds must equal the store's value. That is the property all
// three guards exist to protect; it is checked at quiescence, when any
// remaining disagreement is permanent (there is no TTL here to heal it).
func TestCacheNeverDisagreesWithStoreAfterConcurrentTraffic(t *testing.T) {
	for _, p := range policies {
		t.Run(p.name, func(t *testing.T) {
			s := newFakeStore()
			c := New(s, Options{Capacity: 4, WritePolicy: p.policy})
			var wg sync.WaitGroup
			for g := 0; g < 8; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					rng := rand.New(rand.NewSource(int64(g)))
					for i := 0; i < 400; i++ {
						key := fmt.Sprintf("k%d", rng.Intn(6))
						switch rng.Intn(4) {
						case 0:
							c.Put(key, fmt.Sprintf("v%d-%d", g, i))
						case 1:
							c.Append(key, "+")
						default:
							c.Get(key)
						}
					}
				}(g)
			}
			wg.Wait()

			c.checkInvariants(t)
			c.mu.Lock()
			defer c.mu.Unlock()
			if len(c.inflight) != 0 {
				t.Fatalf("%d flights still registered at quiescence", len(c.inflight))
			}
			for el := c.order.Front(); el != nil; el = el.Next() {
				e := el.Value.(*entry)
				s.mu.Lock()
				want := s.data[e.key]
				s.mu.Unlock()
				if e.value != want {
					t.Fatalf("cache holds %q for %q but the store holds %q", e.value, e.key, want)
				}
			}
		})
	}
}
