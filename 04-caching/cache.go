// Package cache puts a local read cache in front of a KV client — Stage 4
// of the distsys-lab track. See 04-caching/README.md for concept notes and
// 04-caching/TASKS.md for the day-by-day build plan this file follows.
package cache

import (
	"container/list"
	"sync"
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
}

// Cache is a cache-aside read cache: Get answers from local memory when it
// can and otherwise reads the backing Store and remembers the answer. The
// cache is populated lazily by reads, never pre-loaded.
//
// It holds at most capacity entries (Day 2). When a new entry would exceed
// that, the least-recently-used entry is evicted: entries are kept in a
// doubly linked list ordered by recency, with a map from key to list node so
// both lookup and "mark as just used" are O(1). A Get that hits counts as a
// use — a hot key must not be evicted just because it was written long ago.
//
// Still deliberately unfinished:
//   - No expiry (Day 3 adds TTLs).
//   - Writes pass straight through to the Store and DO NOT touch the cache,
//     so a cached key goes stale the moment it is written through this same
//     Cache (Day 4 decides what writes should do to it). See
//     TestWriteLeavesCachedValueStale, which pins that gap down.
//   - Concurrent misses on one key each hit the Store independently
//     (Day 5 coalesces them).
//
// A Cache is safe for concurrent use. Note that a hit now takes the cache's
// exclusive lock, because marking an entry used mutates the recency list — a
// read-only fast path (RWMutex) is not available to an LRU. That lock is held
// only for map and list operations, never across a Store call.
type Cache struct {
	store    Store
	capacity int

	mu      sync.Mutex
	entries map[string]*list.Element // key -> node in order
	order   *list.List               // front = most recently used
	stats   Stats
}

// entry is what each list node holds. The key is stored alongside the value
// so that evicting the back node can also delete it from the map.
type entry struct {
	key   string
	value string
}

// New returns an empty Cache holding at most capacity entries in front of
// store. capacity must be at least 1: a zero-capacity cache would silently be
// a pass-through, and that is a bug in the caller, not a configuration.
func New(store Store, capacity int) *Cache {
	if capacity < 1 {
		panic("cache: capacity must be at least 1")
	}
	return &Cache{
		store:    store,
		capacity: capacity,
		entries:  make(map[string]*list.Element, capacity),
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
		c.order.MoveToFront(el)
		c.stats.Hits++
		v := el.Value.(*entry).value
		c.mu.Unlock()
		return v
	}
	c.stats.Misses++
	c.mu.Unlock()

	v := c.store.Get(key)

	c.mu.Lock()
	c.insertLocked(key, v)
	c.mu.Unlock()
	return v
}

// insertLocked records key=value as the most recently used entry, evicting
// from the back if that pushes the cache over capacity. The key may already
// be present: two goroutines can miss on the same key at once (Day 5 will
// coalesce that), and the second to finish must update the existing node,
// not add a duplicate — a duplicate would leave an unreachable list node
// that later eviction would try to delete from the map by key, removing the
// live entry instead.
func (c *Cache) insertLocked(key, value string) {
	if el, ok := c.entries[key]; ok {
		el.Value.(*entry).value = value
		c.order.MoveToFront(el)
		return
	}
	c.entries[key] = c.order.PushFront(&entry{key: key, value: value})
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
