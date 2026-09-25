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
}

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
// Still deliberately unfinished:
//   - Writes pass straight through to the Store and DO NOT touch the cache,
//     so a cached key goes stale the moment it is written through this same
//     Cache — now for at most one TTL rather than forever. Day 4 decides what
//     writes should do. See TestWriteLeavesCachedValueStale.
//   - Concurrent misses on one key each hit the Store independently
//     (Day 5 coalesces them).
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

	mu      sync.Mutex
	entries map[string]*list.Element // key -> node in order
	order   *list.List               // front = most recently used
	stats   Stats
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
	clock := opts.Clock
	if clock == nil {
		clock = realClock{}
	}
	return &Cache{
		store:    store,
		capacity: opts.Capacity,
		ttl:      opts.TTL,
		clock:    clock,
		entries:  make(map[string]*list.Element, opts.Capacity),
		order:    list.New(),
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
	start := c.nowLocked()
	c.mu.Unlock()

	v := c.store.Get(key)

	c.mu.Lock()
	c.insertLocked(key, v, start)
	c.mu.Unlock()
	return v
}

// nowLocked reads the clock only when a TTL is configured, so a cache
// without expiry pays nothing for the feature.
func (c *Cache) nowLocked() time.Time {
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

// Put writes through to the Store. It does not update or invalidate the
// cache yet — see the Cache doc comment.
func (c *Cache) Put(key, value string) { c.store.Put(key, value) }

// Append writes through to the Store; see Put.
func (c *Cache) Append(key, value string) { c.store.Append(key, value) }

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
