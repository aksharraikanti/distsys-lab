package cache

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
	kvstore "github.com/aksharraikanti/distsys-lab/02-kv-store"
	pool "github.com/aksharraikanti/distsys-lab/03-connection-pooling"
)

func otherPeers(ids []int, self int) []int {
	peers := make([]int, 0, len(ids)-1)
	for _, id := range ids {
		if id != self {
			peers = append(peers, id)
		}
	}
	return peers
}

// pooledStack builds the full stack under the cache — a real-TCP 3-node
// Raft cluster and a PooledClient over it — waits until a leader exists AND
// reads are being served (a fresh leader refuses Get until its own-term
// no-op applies), and returns the client plus a cleanup func. tb is
// testing.TB so tests and benchmarks share it.
func pooledStack(tb testing.TB) (*pool.PooledClient, func()) {
	tb.Helper()
	const n = 3
	transport := raft.NewFakeTransport()
	ids := []int{0, 1, 2}
	nodes := make(map[int]*raft.Raft, n)
	kvs := make([]*kvstore.KVServer, n)
	listeners := make([]net.Listener, n)
	addrs := make([]string, n)
	for _, id := range ids {
		rf := raft.NewRaft(id, otherPeers(ids, id), transport)
		nodes[id] = rf
		transport.Register(id, rf)
		kvs[id] = kvstore.NewKVServer(rf, -1)
		l, err := pool.ServeKVServer("127.0.0.1:0", kvs[id])
		if err != nil {
			tb.Fatalf("ServeKVServer: %v", err)
		}
		listeners[id] = l
		addrs[id] = l.Addr().String()
	}
	for _, rf := range nodes {
		go rf.RunElectionTimer()
		go rf.RunHeartbeats()
		go rf.RunApplyLoop()
	}
	c, err := pool.NewPooledClient(addrs, 2, 4)
	if err != nil {
		tb.Fatalf("NewPooledClient: %v", err)
	}
	cleanup := func() {
		c.Close()
		for _, rf := range nodes {
			rf.StopElectionTimer()
		}
		for _, kv := range kvs {
			kv.Stop()
		}
		for _, l := range listeners {
			l.Close()
		}
	}
	// Client.Get blocks until some leader answers definitively, so one
	// warm-up read is exactly "wait until the stack is serving."
	done := make(chan struct{})
	go func() { c.Get("warmup"); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		cleanup()
		tb.Fatal("stack never started serving reads")
	}
	return c, cleanup
}

// TestCacheOverRealPooledStack: the cache in front of the real thing —
// PooledClient satisfies Store as-is — reads through on a miss and serves
// the repeat from memory.
func TestCacheOverRealPooledStack(t *testing.T) {
	client, cleanup := pooledStack(t)
	defer cleanup()

	c := New(client, Options{Capacity: 100})
	c.Put("greeting", "hello")
	c.Append("greeting", ", world")

	if got := c.Get("greeting"); got != "hello, world" {
		t.Fatalf("Get = %q, want \"hello, world\"", got)
	}
	if got := c.Get("greeting"); got != "hello, world" {
		t.Fatalf("second Get = %q, want \"hello, world\"", got)
	}
	if st := c.Stats(); st.Hits != 1 || st.Misses != 1 {
		t.Fatalf("Stats = %+v, want 1 hit and 1 miss", st)
	}
}

// BenchmarkUncachedGet is the baseline: a pooled Get straight to the cluster.
func BenchmarkUncachedGet(b *testing.B) {
	client, cleanup := pooledStack(b)
	defer cleanup()
	client.Put("bench-key", "x")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		client.Get("bench-key")
	}
}

// BenchmarkCacheMiss reads a distinct key every iteration, so every Get is a
// miss: the price of the cache when it doesn't help (a pooled Get plus the
// bookkeeping).
func BenchmarkCacheMiss(b *testing.B) {
	client, cleanup := pooledStack(b)
	defer cleanup()
	keys := make([]string, b.N)
	for i := range keys {
		keys[i] = fmt.Sprintf("miss-%d", i)
	}
	c := New(client, Options{Capacity: 100})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(keys[i])
	}
}

// BenchmarkCacheHit reads one already-cached key: what the cache is for.
func BenchmarkCacheHit(b *testing.B) {
	client, cleanup := pooledStack(b)
	defer cleanup()
	client.Put("bench-key", "x")
	c := New(client, Options{Capacity: 100})
	c.Get("bench-key") // fill
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get("bench-key")
	}
}

// BenchmarkCacheHitWithTTL is BenchmarkCacheHit with expiry on, i.e. with a
// clock read on every hit. The gap between the two is what TTL checking
// costs; a cache with TTL 0 never touches the clock (see nowLocked).
func BenchmarkCacheHitWithTTL(b *testing.B) {
	client, cleanup := pooledStack(b)
	defer cleanup()
	client.Put("bench-key", "x")
	c := New(client, Options{Capacity: 100, TTL: time.Hour})
	c.Get("bench-key") // fill
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get("bench-key")
	}
}

// TestReadYourWritesOverRealPooledStack: Day 4's guarantee through the real
// thing — Raft cluster, TCP, connection pool — under both policies. Every
// read follows its own goroutine's write, so it must see it.
func TestReadYourWritesOverRealPooledStack(t *testing.T) {
	for _, p := range policies {
		t.Run(p.name, func(t *testing.T) {
			client, cleanup := pooledStack(t)
			defer cleanup()
			c := New(client, Options{Capacity: 50, WritePolicy: p.policy})

			for i := 0; i < 20; i++ {
				key := fmt.Sprintf("key-%d", i%5) // revisit keys so cached entries get overwritten
				want := fmt.Sprintf("v%d", i)
				c.Put(key, want)
				if got := c.Get(key); got != want {
					t.Fatalf("iteration %d: Get(%s) after Put = %q, want %q", i, key, got, want)
				}
				c.Append(key, "!")
				if got := c.Get(key); got != want+"!" {
					t.Fatalf("iteration %d: Get(%s) after Append = %q, want %q", i, key, got, want+"!")
				}
			}
		})
	}
}

// countingStore wraps a Store and counts Get calls that reach it.
type countingStore struct {
	Store
	mu   sync.Mutex
	gets int
}

func (c *countingStore) Get(key string) string {
	c.mu.Lock()
	c.gets++
	c.mu.Unlock()
	return c.Store.Get(key)
}

// TestStampedeOverRealPooledStack: fifty goroutines miss on the same key at
// once against the real Raft/TCP/pool stack. The cluster should see a small
// number of reads — one per "wave" of misses, since a fetch that finishes
// before a late goroutine arrives legitimately lets it hit the cache — and
// never anything like fifty.
func TestStampedeOverRealPooledStack(t *testing.T) {
	client, cleanup := pooledStack(t)
	defer cleanup()
	client.Put("hot", "value")
	cs := &countingStore{Store: client}
	c := New(cs, Options{Capacity: 10})

	const callers = 50
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if got := c.Get("hot"); got != "value" {
				t.Errorf("Get = %q, want value", got)
			}
		}()
	}
	close(start)
	wg.Wait()

	cs.mu.Lock()
	gets := cs.gets
	cs.mu.Unlock()
	st := c.Stats()
	t.Logf("%d concurrent Gets of one key -> %d cluster reads (hits=%d misses=%d coalesced=%d)", callers, gets, st.Hits, st.Misses, st.Coalesced)
	if gets >= callers/2 {
		t.Fatalf("%d cluster reads for %d concurrent misses on one key; coalescing should have collapsed them", gets, callers)
	}
}
