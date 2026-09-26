package cache

import (
	"fmt"
	"sync"
	"testing"
)

// fakeStore is an in-memory Store that counts calls, so tests can assert
// exactly when the cache does and doesn't reach the backing store.
type fakeStore struct {
	mu   sync.Mutex
	data map[string]string
	gets int
}

func newFakeStore() *fakeStore { return &fakeStore{data: make(map[string]string)} }

func (s *fakeStore) Get(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	return s.data[key]
}
func (s *fakeStore) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}
func (s *fakeStore) Append(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] += value
}
func (s *fakeStore) getCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.gets
}

func TestGetMissThenHit(t *testing.T) {
	s := newFakeStore()
	s.Put("k", "v")
	c := New(s, Options{Capacity: 100})

	if got := c.Get("k"); got != "v" {
		t.Fatalf("first Get = %q, want v", got)
	}
	if got := c.Get("k"); got != "v" {
		t.Fatalf("second Get = %q, want v", got)
	}
	if got := s.getCalls(); got != 1 {
		t.Fatalf("store Get called %d times, want 1 (second read must be a hit)", got)
	}
	if st := c.Stats(); st.Hits != 1 || st.Misses != 1 {
		t.Fatalf("Stats = %+v, want 1 hit and 1 miss", st)
	}
}

// A missing key reads as "", and that answer is cached too — otherwise every
// read of an absent key would be a permanent miss.
func TestMissingKeyIsCachedAsEmpty(t *testing.T) {
	s := newFakeStore()
	c := New(s, Options{Capacity: 100})
	for i := 0; i < 3; i++ {
		if got := c.Get("absent"); got != "" {
			t.Fatalf("Get(absent) = %q, want \"\"", got)
		}
	}
	if got := s.getCalls(); got != 1 {
		t.Fatalf("store Get called %d times for a missing key read 3 times, want 1", got)
	}
}

func TestWritesPassThroughToStore(t *testing.T) {
	s := newFakeStore()
	c := New(s, Options{Capacity: 100})
	c.Put("k", "a")
	c.Append("k", "b")
	if got := s.data["k"]; got != "ab" {
		t.Fatalf("store holds %q, want \"ab\" (Put then Append must both reach the store)", got)
	}
}

func TestConcurrentGetsAreRaceFree(t *testing.T) {
	s := newFakeStore()
	for i := 0; i < 20; i++ {
		s.Put(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))
	}
	c := New(s, Options{Capacity: 100})

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				k := fmt.Sprintf("k%d", i%20)
				if got, want := c.Get(k), fmt.Sprintf("v%d", i%20); got != want {
					t.Errorf("Get(%s) = %q, want %q", k, got, want)
					return
				}
			}
		}()
	}
	wg.Wait()
	if st := c.Stats(); st.Hits+st.Misses != 16*200 {
		t.Fatalf("hits+misses = %d, want %d — every Get must be counted exactly once", st.Hits+st.Misses, 16*200)
	}
}
