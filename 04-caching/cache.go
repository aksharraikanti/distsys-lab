// Package cache puts a local read cache in front of a KV client — Stage 4
// of the distsys-lab track. See 04-caching/README.md for concept notes and
// 04-caching/TASKS.md for the day-by-day build plan this file follows.
package cache

import "sync"

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
	Hits   int64
	Misses int64
}

// Cache is a cache-aside read cache: Get answers from a local map when it
// can and otherwise reads the backing Store and remembers the answer. The
// APPLICATION-visible policy is cache-aside — the cache is populated lazily
// by reads, never pre-loaded.
//
// Day 1 is deliberately the simplest thing that is still a cache:
//   - Unbounded (Day 2 adds capacity and LRU eviction).
//   - No expiry (Day 3 adds TTLs).
//   - Writes pass straight through to the Store and DO NOT touch the cache,
//     so a cached key goes stale the moment it is written through this same
//     Cache (Day 4 decides what writes should do to it). See
//     TestWriteLeavesCachedValueStale, which pins that gap down.
//   - Concurrent misses on one key each hit the Store independently
//     (Day 5 coalesces them).
//
// A Cache is safe for concurrent use.
type Cache struct {
	store Store

	mu      sync.Mutex
	entries map[string]string
	stats   Stats
}

// New returns an empty Cache in front of store.
func New(store Store) *Cache {
	return &Cache{store: store, entries: make(map[string]string)}
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
// on Day 1: if the key is created later, the cached "" is served until
// something evicts it.
func (c *Cache) Get(key string) string {
	c.mu.Lock()
	if v, ok := c.entries[key]; ok {
		c.stats.Hits++
		c.mu.Unlock()
		return v
	}
	c.stats.Misses++
	c.mu.Unlock()

	v := c.store.Get(key)

	c.mu.Lock()
	c.entries[key] = v
	c.mu.Unlock()
	return v
}

// Put writes through to the Store. It does not update or invalidate the
// cache on Day 1 — see the Cache doc comment.
func (c *Cache) Put(key, value string) { c.store.Put(key, value) }

// Append writes through to the Store; see Put.
func (c *Cache) Append(key, value string) { c.store.Append(key, value) }

// Stats returns a snapshot of the hit/miss counters.
func (c *Cache) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}
