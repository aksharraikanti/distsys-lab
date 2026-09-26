package cache

import (
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"regexp"
	"sync"
	"testing"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
	kvstore "github.com/aksharraikanti/distsys-lab/02-kv-store"
	pool "github.com/aksharraikanti/distsys-lab/03-connection-pooling"
)

// crashableEndpoint is a KVServer's real TCP face with a working
// crash()/start(): crash refuses new connections AND severs every established
// one, which is what a client of a really-crashed process sees. It is a copy
// of the helper in 03-connection-pooling's load_test.go — unexported test code
// can't be imported across packages, and duplicating ~50 lines is simpler than
// exporting a fault-injection API from a production package.
type crashableEndpoint struct {
	kv *kvstore.KVServer

	mu    sync.Mutex
	addr  string
	l     net.Listener
	conns []net.Conn
}

func (e *crashableEndpoint) start() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	l, err := net.Listen("tcp", e.addr)
	if err != nil {
		return err
	}
	e.l = l
	e.addr = l.Addr().String()
	e.conns = nil

	server := rpc.NewServer()
	if err := server.RegisterName("KVServer", e.kv); err != nil {
		l.Close()
		return err
	}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			e.mu.Lock()
			if e.l != l {
				e.mu.Unlock()
				c.Close()
				return
			}
			e.conns = append(e.conns, c)
			e.mu.Unlock()
			go server.ServeConn(c)
		}
	}()
	return nil
}

func (e *crashableEndpoint) crash() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.l == nil {
		return
	}
	e.l.Close()
	e.l = nil
	for _, c := range e.conns {
		c.Close()
	}
	e.conns = nil
}

// faultStack is pooledStack with the fault surfaces exposed: the Raft
// transport (leader cut-off, partitions) and each node's client-facing TCP
// endpoint (crash/restart).
type faultStack struct {
	client    *pool.PooledClient
	nodes     map[int]*raft.Raft
	eps       []*crashableEndpoint
	transport *raft.FakeTransport
	cleanup   func()
}

func newFaultStack(tb testing.TB) *faultStack {
	tb.Helper()
	const n = 3
	transport := raft.NewFakeTransport()
	ids := []int{0, 1, 2}
	nodes := make(map[int]*raft.Raft, n)
	kvs := make([]*kvstore.KVServer, n)
	eps := make([]*crashableEndpoint, n)
	addrs := make([]string, n)
	for _, id := range ids {
		rf := raft.NewRaft(id, otherPeers(ids, id), transport)
		nodes[id] = rf
		transport.Register(id, rf)
		kvs[id] = kvstore.NewKVServer(rf, -1)
		eps[id] = &crashableEndpoint{kv: kvs[id], addr: "127.0.0.1:0"}
		if err := eps[id].start(); err != nil {
			tb.Fatalf("endpoint %d: %v", id, err)
		}
		addrs[id] = eps[id].addr
	}
	for _, rf := range nodes {
		go rf.RunElectionTimer()
		go rf.RunHeartbeats()
		go rf.RunApplyLoop()
	}
	client, err := pool.NewPooledClient(addrs, 1, 4)
	if err != nil {
		tb.Fatalf("NewPooledClient: %v", err)
	}
	fs := &faultStack{client: client, nodes: nodes, eps: eps, transport: transport}
	fs.cleanup = func() {
		client.Close()
		for _, rf := range nodes {
			rf.StopElectionTimer()
		}
		for _, kv := range kvs {
			kv.Stop()
		}
		for _, e := range eps {
			e.crash()
		}
	}
	// Wait until the cluster is actually serving reads.
	done := make(chan struct{})
	go func() { client.Get("warmup"); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		fs.cleanup()
		tb.Fatal("stack never started serving reads")
	}
	return fs
}

// TestCacheUpholdsItsInvariantsThroughFaults: the stage's full-integration
// test. Concurrent goroutines share ONE cache over ONE pooled client against a
// real-TCP Raft cluster while three faults fire in rotation — a node's TCP
// endpoint crashing (every connection severed), the Raft leader cut off, a
// majority/minority partition.
//
// Two kinds of goroutine, under both write policies:
//   - Writers each own a key. After every Put and Append they read it back and
//     must see exactly what they wrote (read-your-writes).
//   - Readers only read, continuously, from two sets: shared keys with
//     CONSTANT values (which must never read as anything else) and the
//     writers' own keys (whose value they can't predict, but which must always
//     be well-formed). The readers matter as much as the writers: they are
//     what races the cache's fills against the writers' updates, and what
//     makes concurrent misses on the same key actually happen — without them
//     writes serialize and the stale-fill and coalescing guards are never
//     exercised. (An earlier version had only writers, and its stats showed
//     coalesced=0 staleDiscarded=0: it passed without touching either guard.)
//
// As in Stage 3's load test, the injector fires immediately and the test
// asserts that at least three fault rounds ran.
func TestCacheUpholdsItsInvariantsThroughFaults(t *testing.T) {
	wellFormed := regexp.MustCompile(`^(v[0-9]+\+?)?$`) // "" before the first write, then vN or vN+

	for _, p := range policies {
		t.Run(p.name, func(t *testing.T) {
			const writers, readers, iterations, sharedKeys = 4, 4, 100, 8

			fs := newFaultStack(t)
			defer fs.cleanup()
			for i := 0; i < sharedKeys; i++ { // seeded BEFORE any fault
				fs.client.Put(fmt.Sprintf("shared-%d", i), fmt.Sprintf("const-%d", i))
			}
			// Small capacity and a short TTL: entries are constantly evicted
			// AND expiring, so fills, expirations and refetches all happen
			// while the faults run.
			c := New(fs.client, Options{Capacity: 6, TTL: 15 * time.Millisecond, WritePolicy: p.policy})

			faultStop := make(chan struct{})
			var faultWg sync.WaitGroup
			faultWg.Add(1)
			faultRounds := 0
			go func() {
				defer faultWg.Done()
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				for round := 0; ; round++ {
					select {
					case <-faultStop:
						return
					case <-time.After(2 * raft.ElectionTimeoutMax):
					}
					faultRounds++
					switch round % 3 {
					case 0:
						e := fs.eps[rng.Intn(len(fs.eps))]
						e.crash()
						time.Sleep(3 * raft.ElectionTimeoutMax)
						if err := e.start(); err != nil {
							t.Errorf("endpoint restart: %v", err)
						}
					case 1:
						for id, rf := range fs.nodes {
							if rf.State() == raft.Leader {
								fs.transport.Unregister(id)
								time.Sleep(2 * raft.ElectionTimeoutMax)
								fs.transport.Register(id, rf)
								break
							}
						}
					case 2:
						fs.transport.Partition([]int{0, 1}, []int{2})
						time.Sleep(3 * raft.ElectionTimeoutMax)
						fs.transport.Heal()
					}
				}
			}()

			errs := make(chan error, writers+readers)
			readStop := make(chan struct{})
			var readWg, writeWg sync.WaitGroup

			for r := 0; r < readers; r++ {
				readWg.Add(1)
				go func(r int) {
					defer readWg.Done()
					rng := rand.New(rand.NewSource(int64(1000 + r)))
					next := zipfKeys(int64(r), sharedKeys)
					for {
						select {
						case <-readStop:
							return
						case <-time.After(100 * time.Microsecond):
							// Throttled on purpose: an unthrottled reader is a
							// busy loop that pins a core for the whole run,
							// and under `go test ./...` that starved Stage 1's
							// timing-sensitive tests running in parallel.
						}
						if rng.Intn(2) == 0 {
							var n int
							fmt.Sscanf(next(), "key-%d", &n)
							key, want := fmt.Sprintf("shared-%d", n), fmt.Sprintf("const-%d", n)
							if got := c.Get(key); got != want {
								errs <- fmt.Errorf("reader %d: constant key %s read back %q, want %q", r, key, got, want)
								return
							}
						} else {
							key := fmt.Sprintf("own-%d", rng.Intn(writers))
							if got := c.Get(key); !wellFormed.MatchString(got) {
								errs <- fmt.Errorf("reader %d: %s read back malformed value %q", r, key, got)
								return
							}
						}
					}
				}(r)
			}

			for w := 0; w < writers; w++ {
				writeWg.Add(1)
				go func(w int) {
					defer writeWg.Done()
					own := fmt.Sprintf("own-%d", w)
					for i := 0; i < iterations; i++ {
						want := fmt.Sprintf("v%d", i)
						c.Put(own, want)
						if got := c.Get(own); got != want {
							errs <- fmt.Errorf("writer %d iter %d: Get(%s) after Put = %q, want %q", w, i, own, got, want)
							return
						}
						c.Append(own, "+")
						if got := c.Get(own); got != want+"+" {
							errs <- fmt.Errorf("writer %d iter %d: Get(%s) after Append = %q, want %q", w, i, own, got, want+"+")
							return
						}
					}
				}(w)
			}

			writeWg.Wait()
			close(readStop)
			readWg.Wait()
			close(faultStop)
			faultWg.Wait()
			close(errs)
			for err := range errs {
				t.Error(err)
			}
			if faultRounds < 3 {
				t.Errorf("only %d fault rounds ran, want >= 3: the workload finished too fast to test anything", faultRounds)
			}
			st := c.Stats()
			t.Logf("%d fault rounds; hits=%d misses=%d evictions=%d expirations=%d coalesced=%d staleDiscarded=%d",
				faultRounds, st.Hits, st.Misses, st.Evictions, st.Expirations, st.Coalesced, st.StaleFillsDiscarded)
			if st.Hits == 0 || st.Evictions == 0 || st.Expirations == 0 {
				t.Errorf("the run did not exercise the cache (hits=%d evictions=%d expirations=%d)", st.Hits, st.Evictions, st.Expirations)
			}
			c.checkInvariants(t)
		})
	}
}

// TestCacheServesHitsDuringTotalStoreOutage: what "degrades rather than
// wedges" can honestly mean here. Store has no error return — the KV client
// retries forever — so during a full outage an UNCACHED read or a write simply
// waits. The cache's job is to make sure that waiting stays contained: hits
// keep being answered from memory, and one blocked writer or fetcher doesn't
// hold any lock a reader needs. Then, when the store returns, everything that
// was waiting completes.
func TestCacheServesHitsDuringTotalStoreOutage(t *testing.T) {
	fs := newFaultStack(t)
	defer fs.cleanup()
	c := New(fs.client, Options{Capacity: 10})
	c.Put("cached", "value")
	c.Get("cached") // resident (invalidate policy: this read fills it)

	for _, e := range fs.eps {
		e.crash()
	}

	// An uncached read and a write both start during the outage and block.
	fetch := make(chan string, 1)
	go func() { fetch <- c.Get("uncached") }()
	writeDone := make(chan struct{})
	go func() { c.Put("other", "x"); close(writeDone) }()
	waitUntil(t, "the blocked fetch to register", func() bool { return c.Stats().Misses >= 2 })
	time.Sleep(50 * time.Millisecond) // let them actually be stuck in the store call

	// Hits are unaffected: answered from memory, immediately, while a fetch
	// and a write are both stuck in the store.
	for i := 0; i < 100; i++ {
		got := make(chan string, 1)
		go func() { got <- c.Get("cached") }()
		select {
		case v := <-got:
			if v != "value" {
				t.Fatalf("hit during outage = %q, want value", v)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("a cache HIT blocked during a store outage: some lock is held across a store call")
		}
	}
	select {
	case <-fetch:
		t.Fatal("the uncached read returned during a total outage; the test isn't outaging anything")
	case <-writeDone:
		t.Fatal("the write completed during a total outage; the test isn't outaging anything")
	default:
	}

	// Bring the store back: the stuck fetch and write must both complete.
	for _, e := range fs.eps {
		if err := e.start(); err != nil {
			t.Fatalf("restart: %v", err)
		}
	}
	select {
	case <-writeDone:
	case <-time.After(15 * time.Second):
		t.Fatal("the blocked write never completed after the store came back")
	}
	select {
	case <-fetch:
	case <-time.After(15 * time.Second):
		t.Fatal("the blocked fetch never completed after the store came back")
	}
	if got := c.Get("other"); got != "x" {
		t.Fatalf("Get(other) after recovery = %q, want x", got)
	}
	c.checkInvariants(t)
}
