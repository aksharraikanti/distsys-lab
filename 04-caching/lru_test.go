package cache

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"
)

// keysMRUFirst returns the resident keys, most recently used first.
func (c *Cache) keysMRUFirst() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var keys []string
	for el := c.order.Front(); el != nil; el = el.Next() {
		keys = append(keys, el.Value.(*entry).key)
	}
	return keys
}

// checkInvariants verifies the map and the recency list agree exactly: same
// size, and every list node is the one its key maps to. A mismatch is the
// signature bug of a map+list LRU — an orphaned node that eviction later
// "removes" by deleting the wrong live entry.
func (c *Cache) checkInvariants(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) != c.order.Len() {
		t.Fatalf("map has %d entries but list has %d nodes", len(c.entries), c.order.Len())
	}
	if c.order.Len() > c.capacity {
		t.Fatalf("holds %d entries, capacity is %d", c.order.Len(), c.capacity)
	}
	for el := c.order.Front(); el != nil; el = el.Next() {
		if c.entries[el.Value.(*entry).key] != el {
			t.Fatalf("list node for %q is not the node its key maps to", el.Value.(*entry).key)
		}
	}
}

func TestEvictsLeastRecentlyUsed(t *testing.T) {
	s := newFakeStore()
	c := New(s, Options{Capacity: 3})
	for _, k := range []string{"a", "b", "c", "d"} { // d pushes out a
		c.Get(k)
	}
	if got, want := c.keysMRUFirst(), []string{"d", "c", "b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("resident = %v, want %v", got, want)
	}
	if st := c.Stats(); st.Evictions != 1 {
		t.Fatalf("Evictions = %d, want 1", st.Evictions)
	}

	before := s.getCalls()
	c.Get("b")
	c.Get("c")
	c.Get("d")
	if s.getCalls() != before {
		t.Fatalf("resident keys must be hits; store Get was called %d more times", s.getCalls()-before)
	}
	c.Get("a") // evicted earlier, so this must reach the store again
	if s.getCalls() != before+1 {
		t.Fatalf("evicted key must be re-fetched from the store")
	}
}

// A hit must count as a use, or eviction order would be plain FIFO and a hot
// key would be thrown out just for having been loaded early.
func TestGetCountsAsUse(t *testing.T) {
	c := New(newFakeStore(), Options{Capacity: 2})
	c.Get("a")
	c.Get("b")
	c.Get("a") // touch a: b is now the least recently used
	c.Get("c") // evicts b, not a
	if got, want := c.keysMRUFirst(), []string{"c", "a"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("resident = %v, want %v (b should have been evicted, a kept)", got, want)
	}
}

func TestEvictionOrderOverLongerSequence(t *testing.T) {
	c := New(newFakeStore(), Options{Capacity: 3})
	steps := []struct {
		get  string
		want []string // MRU first, after this Get
	}{
		{"a", []string{"a"}},
		{"b", []string{"b", "a"}},
		{"c", []string{"c", "b", "a"}},
		{"a", []string{"a", "c", "b"}}, // touch
		{"d", []string{"d", "a", "c"}}, // evicts b
		{"c", []string{"c", "d", "a"}}, // touch
		{"e", []string{"e", "c", "d"}}, // evicts a
		{"b", []string{"b", "e", "c"}}, // b was evicted before: miss, evicts d
	}
	for i, st := range steps {
		c.Get(st.get)
		if got := c.keysMRUFirst(); !reflect.DeepEqual(got, st.want) {
			t.Fatalf("step %d (Get %q): resident = %v, want %v", i, st.get, got, st.want)
		}
		c.checkInvariants(t)
	}
}

func TestNeverExceedsCapacityUnderConcurrency(t *testing.T) {
	const capacity = 8
	c := New(newFakeStore(), Options{Capacity: capacity})
	var wg sync.WaitGroup
	for g := 0; g < 12; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				c.Get(fmt.Sprintf("k%d", (g*7+i)%40)) // 40 keys through 8 slots
			}
		}(g)
	}
	wg.Wait()
	c.checkInvariants(t)
	if c.Len() != capacity {
		t.Fatalf("Len = %d, want %d (40 distinct keys passed through a full cache)", c.Len(), capacity)
	}
}

// gatedStore blocks every Get until released, so a test can force several
// callers to be in the middle of a miss at the same time.
type gatedStore struct {
	*fakeStore
	entered chan struct{}
	release chan struct{}
}

func (g *gatedStore) Get(key string) string {
	g.entered <- struct{}{}
	<-g.release
	return g.fakeStore.Get(key)
}

// insertLocked must UPDATE an existing key's node, never push a second node
// for it: a duplicate leaves an orphan the map doesn't point at, and evicting
// that orphan deletes the live entry by key. Concurrent misses used to reach
// this branch directly; since Day 5 coalesces them it is defensive, so the
// test now drives it white-box.
func TestInsertOfAnExistingKeyUpdatesInPlace(t *testing.T) {
	c := New(newFakeStore(), Options{Capacity: 2})
	c.mu.Lock()
	c.insertLocked("k", "v1", time.Time{})
	c.insertLocked("k", "v2", time.Time{})
	c.mu.Unlock()

	c.checkInvariants(t)
	if c.Len() != 1 {
		t.Fatalf("Len = %d after inserting one key twice, want 1", c.Len())
	}
	if got := c.Get("k"); got != "v2" {
		t.Fatalf("Get = %q, want v2 (the second insert must win)", got)
	}
	for _, k := range []string{"x", "y", "z"} { // force evictions
		c.Get(k)
	}
	c.checkInvariants(t)
}

func TestNewRejectsNonPositiveCapacity(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("New(store, Options{Capacity: 0}) did not panic")
		}
	}()
	New(newFakeStore(), Options{Capacity: 0})
}
