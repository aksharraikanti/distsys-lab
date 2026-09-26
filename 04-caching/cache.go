// Package cache puts a local read cache in front of a KV client — Stage 4
// of the distsys-lab track. See 04-caching/README.md for concept notes and
// 04-caching/TASKS.md for the day-by-day build plan this file follows.
package cache

import (
	"container/list"
	"sync"
	"time"
)

// Store is what a Cache sits in front of: the three operations every KV
// client in this repo already has. 03-connection-pooling's PooledClient
// satisfies it as-is; tests substitute a fake with controllable latency and
// call counts. Implementations must be safe for concurrent use, because a
// Cache shares one Store across all of its callers — which is why Stage 4
// Day 1 had to make PooledClient safe for concurrent use first.
type Store interface {
	Get(key string) string
	Put(key, value string)
	Append(key, value string)
}

// Stats is a snapshot of a Cache's counters.
type Stats struct {
	Hits      int64
	Misses    int64
	Evictions int64
	// Expirations counts entries found past their TTL when read. Each one
	// is also counted as a Miss — an expired entry is not a hit.
	Expirations int64
	// Invalidations counts entries a write dropped from the cache (Day 4).
	Invalidations int64
	// Coalesced counts Gets that found a fetch of their key already in
	// flight and waited for it instead of hitting the Store (Day 5). Each is
	// also counted as a Miss.
	Coalesced int64
	// StaleFillsDiscarded counts fetch results thrown away instead of cached
	// because a write to the key landed while the fetch was in flight (Day 5).
	StaleFillsDiscarded int64
}

// WritePolicy is what a Put does to the cache (Day 4). Append always
// invalidates under either policy — see Cache.Append.
type WritePolicy int

const (
	// WriteInvalidate drops the key's entry on a write, so the next read
	// misses and refills from the store. It is the zero value: it can never
	// cache a wrong value, it only costs a miss.
	WriteInvalidate WritePolicy = iota

	// WriteThrough replaces the key's entry with the value just written (and
	// populates it if absent), so a read right after a write is a hit. The
	// costs are that every write now displaces something in a bounded cache
	// — including writes to keys nobody ever reads — and that it can only be
	// as right as the write itself.
	WriteThrough
)

// Clock is the cache's source of time. It exists so tests can control it:
// expiry tested against real sleeps is slow and flaky (this repo's history
// with Stage 1's election timeouts and Stage 3's idle evictor is exactly
// that), while expiry tested against a clock the test advances by hand is
// instant and exact.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Options configures a Cache. A struct rather than positional parameters for
// the reason Stage 3 Day 5 learned the hard way: New was about to have
// several same-shaped arguments in a row, which the compiler can't catch if
// two are swapped.
type Options struct {
	// Capacity is the most entries the cache holds (Day 2). Required, >= 1.
	Capacity int

	// TTL is how long an entry may be served after it was fetched (Day 3).
	// Zero means entries never expire. It is measured from when the fetch
	// STARTED and is not extended by hits — see Cache.
	TTL time.Duration

	// Clock supplies the time for TTL checks; nil means the real clock.
	Clock Clock

	// WritePolicy is what Put does to the cache (Day 4). The zero value is
	// WriteInvalidate.
	WritePolicy WritePolicy
}

// Cache is a cache-aside read cache: Get answers from local memory when it
// can and otherwise reads the backing Store and remembers the answer. The
// cache is populated lazily by reads, never pre-loaded.
//
// It holds at most Options.Capacity entries (Day 2). When a new entry would
// exceed that, the least-recently-used entry is evicted: entries are kept in
// a doubly linked list ordered by recency, with a map from key to list node
// so both lookup and "mark as just used" are O(1). A Get that hits counts as
// a use — a hot key must not be evicted just because it was loaded long ago.
//
// Entries also expire (Day 3). Even a cache whose own writes were handled
// perfectly would go stale, because other clients can change a key without
// going through this Cache; a TTL is the only thing that bounds how long
// such a value can be served. Three deliberate choices:
//   - The deadline is fetch-start + TTL, not fetch-end + TTL: a slow fetch
//     returned a value that was current at some point during the fetch, so
//     counting from the end would overstate how fresh it is.
//   - A hit does NOT extend the deadline. Refreshing on read would let a hot
//     key stay cached, and stale, forever — the keys read most are exactly
//     the ones whose staleness is most visible.
//   - Expiry is lazy: an entry is checked when it is read, and removed then.
//     There is no background sweeper (a goroutine with a lifecycle, for
//     memory that Capacity already bounds). An expired entry nobody reads
//     again just sits until LRU evicts it; it never gets served.
//
// Writes (Day 4) go to the Store FIRST and only then touch the cache, per
// Options.WritePolicy, so a single goroutine always reads its own writes.
// Store-first is deliberate: invalidating first would let a concurrent reader
// refill the cache with the OLD value in the gap before the store write lands.
//
// Three concurrency problems are handled explicitly (Day 5):
//   - Stampede: concurrent misses on one key are coalesced into a single
//     Store fetch. Without it a hot key that expires (or is evicted) sends
//     every waiting caller to the Store at once — the moment the Store is
//     least able to take it.
//   - Stale fill: a reader that fetched the old value before a write must not
//     insert it after the write. Each miss registers an in-flight fetch; a
//     write marks that fetch stale, so its result is returned to its own
//     caller (whose Get overlapped the write, so old is legal) but never
//     cached. The write also DETACHES the fetch, so a Get that starts after
//     the write begins a fresh one rather than joining the stale one and
//     reading its own write back as the old value.
//   - Write reordering: two concurrent writers to one key could land in the
//     Store in one order and update the cache in the other, leaving the cache
//     permanently disagreeing with the store under WriteThrough. Writes are
//     therefore serialized (writeMu). Readers never take that lock.
//
// A Cache is safe for concurrent use. Note that a hit takes the cache's
// exclusive lock, because marking an entry used mutates the recency list — a
// read-only fast path (RWMutex) is not available to an LRU. That lock is held
// only for map and list operations, never across a Store call.
type Cache struct {
	store    Store
	capacity int
	ttl      time.Duration
	clock    Clock
	policy   WritePolicy

	// writeMu serializes writes so cache updates happen in the same order as
	// Store writes. It is held across the Store call, so a slow write delays
	// other writers — never readers. (The KV client already serializes writes
	// per ClientID, so through it this adds no new serialization.)
	writeMu sync.Mutex

	mu       sync.Mutex
	entries  map[string]*list.Element // key -> node in order
	order    *list.List               // front = most recently used
	inflight map[string]*flight       // fetches currently running, by key
	stats    Stats
}

// flight is one in-flight Store fetch for a key, shared by every Get that
// missed on that key while it ran.
type flight struct {
	done  chan struct{} // closed when the fetch finishes, successfully or not
	value string        // valid once done is closed, if ok
	ok    bool          // false if the fetch panicked; waiters then retry
	stale bool          // a write landed during the fetch: do not cache value
}

// entry is what each list node holds. The key is stored alongside the value
// so that evicting the back node can also delete it from the map.
type entry struct {
	key     string
	value   string
	expires time.Time // zero = never expires
}

// New returns an empty Cache in front of store. It panics on invalid
// options — a zero-capacity cache would silently be a pass-through and a
// negative TTL would silently make everything expired; both are bugs in the
// caller, not configuration.
func New(store Store, opts Options) *Cache {
	if opts.Capacity < 1 {
		panic("cache: Options.Capacity must be at least 1")
	}
	if opts.TTL < 0 {
		panic("cache: Options.TTL must not be negative")
	}
	if opts.WritePolicy != WriteInvalidate && opts.WritePolicy != WriteThrough {
		panic("cache: unknown Options.WritePolicy")
	}
	clock := opts.Clock
	if clock == nil {
		clock = realClock{}
	}
	return &Cache{
		store:    store,
		capacity: opts.Capacity,
		ttl:      opts.TTL,
		clock:    clock,
		policy:   opts.WritePolicy,
		entries:  make(map[string]*list.Element, opts.Capacity),
		order:    list.New(),
		inflight: make(map[string]*flight),
	}
}

// Get returns the value for key, from the cache if present.
//
// The Store is called WITHOUT holding the cache lock. Holding it would make
// one slow miss block every other caller — including callers whose keys are
// already cached and would have been instant hits — turning the cache into
// a global serialization point exactly when the backing store is slow.
//
// An absent key reads as "" (the Store's own convention) and that answer is
// cached like any other, so repeated reads of a missing key don't each pay a
// round trip either. The flip side is the same staleness as everything else
// here: if the key is created later, the cached "" is served until something
// evicts it.
func (c *Cache) Get(key string) string {
	for {
		c.mu.Lock()
		if el, ok := c.entries[key]; ok {
			e := el.Value.(*entry)
			if !c.expiredLocked(e) {
				c.order.MoveToFront(el)
				c.stats.Hits++
				v := e.value
				c.mu.Unlock()
				return v
			}
			// Expired: drop it and fall through to an ordinary miss.
			c.order.Remove(el)
			delete(c.entries, key)
			c.stats.Expirations++
		}
		c.stats.Misses++

		if f, ok := c.inflight[key]; ok {
			// Someone is already fetching this key: wait for their answer
			// instead of asking the Store again.
			c.stats.Coalesced++
			c.mu.Unlock()
			<-f.done
			if f.ok {
				return f.value
			}
			continue // the leader's fetch panicked; try again, possibly as leader
		}

		f := &flight{done: make(chan struct{})}
		c.inflight[key] = f
		start := c.now()
		c.mu.Unlock()
		return c.lead(key, f, start)
	}
}

// lead runs the Store fetch for a flight this goroutine registered, caches the
// result unless a write made it stale, and wakes everyone who joined. The
// cleanup is deferred so that waiters are released even if the Store panics —
// they then retry rather than blocking forever.
//
// Registration happens (under the lock) BEFORE the Store is read. That
// ordering is what makes the stale mark sound: any fetch that could have read
// a value from before a write registered before that write landed, hence
// before the write took the lock to mark it.
func (c *Cache) lead(key string, f *flight, start time.Time) string {
	defer func() {
		c.mu.Lock()
		if c.inflight[key] == f { // a write may already have detached it
			delete(c.inflight, key)
		}
		c.mu.Unlock()
		close(f.done)
	}()

	v := c.store.Get(key)

	c.mu.Lock()
	f.value, f.ok = v, true
	if f.stale {
		c.stats.StaleFillsDiscarded++
	} else {
		c.insertLocked(key, v, start)
	}
	c.mu.Unlock()
	return v
}

// markStaleLocked is called by every write after its Store write has landed.
// A fetch in flight for key may have read the old value, so it must not be
// cached; and it is detached so that Gets arriving after this write start a
// fresh fetch instead of joining it and being handed the pre-write value.
func (c *Cache) markStaleLocked(key string) {
	if f, ok := c.inflight[key]; ok {
		f.stale = true
		delete(c.inflight, key)
	}
}

// now reads the clock only when a TTL is configured, so a cache
// without expiry pays nothing for the feature.
func (c *Cache) now() time.Time {
	if c.ttl == 0 {
		return time.Time{}
	}
	return c.clock.Now()
}

// expiredLocked reports whether e is at or past its deadline. At exactly the
// deadline the entry is expired: a TTL of d means "served for less than d".
func (c *Cache) expiredLocked(e *entry) bool {
	if c.ttl == 0 {
		return false
	}
	return !c.clock.Now().Before(e.expires)
}

// insertLocked records key=value as the most recently used entry, evicting
// from the back if that pushes the cache over capacity. The key may already
// be present: two goroutines can miss on the same key at once (Day 5 will
// coalesce that), and the second to finish must update the existing node,
// not add a duplicate — a duplicate would leave an unreachable list node
// that later eviction would try to delete from the map by key, removing the
// live entry instead.
func (c *Cache) insertLocked(key, value string, fetchStart time.Time) {
	var expires time.Time
	if c.ttl > 0 {
		expires = fetchStart.Add(c.ttl)
	}
	if el, ok := c.entries[key]; ok {
		e := el.Value.(*entry)
		e.value = value
		e.expires = expires
		c.order.MoveToFront(el)
		return
	}
	c.entries[key] = c.order.PushFront(&entry{key: key, value: value, expires: expires})
	if c.order.Len() > c.capacity {
		oldest := c.order.Back()
		c.order.Remove(oldest)
		delete(c.entries, oldest.Value.(*entry).key)
		c.stats.Evictions++
	}
}

// Put writes to the Store, then updates the cache per the WritePolicy: drop
// the entry (WriteInvalidate) or replace it with value (WriteThrough). The
// store write happens first — see the Cache doc comment for why.
//
// Under WriteThrough the entry's TTL counts from when the write STARTED, the
// same conservative rule reads use: the store acknowledged some time after
// that, and another client may have written since.
func (c *Cache) Put(key, value string) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	start := c.now()
	c.store.Put(key, value)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.markStaleLocked(key)
	if c.policy == WriteThrough {
		c.insertLocked(key, value, start)
		return
	}
	c.invalidateLocked(key)
}

// Append writes to the Store, then drops the key's cache entry — under BOTH
// policies. The resulting value is old+value, but the cache cannot safely
// compute that: its copy may be stale (TTL, or another client's write), and
// appending to a stale value caches something that never existed in the
// store. Reading the result back would make it correct, but it costs a round
// trip on every Append to prefetch a value that may never be read — exactly
// the cost write-through exists to avoid. So Append invalidates, and the next
// read pays one honest miss.
func (c *Cache) Append(key, value string) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.store.Append(key, value)

	c.mu.Lock()
	defer c.mu.Unlock()
	c.markStaleLocked(key)
	c.invalidateLocked(key)
}

// invalidateLocked drops key's entry if present, counting it only when there
// was actually something to drop.
func (c *Cache) invalidateLocked(key string) {
	if el, ok := c.entries[key]; ok {
		c.order.Remove(el)
		delete(c.entries, key)
		c.stats.Invalidations++
	}
}

// Len returns how many entries the cache currently holds.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.order.Len()
}

// Stats returns a snapshot of the counters.
func (c *Cache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}
