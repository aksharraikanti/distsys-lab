package pool

import (
	"errors"
	"fmt"
	"net/rpc"
	"sync"
	"testing"
	"time"

	kvstore "github.com/aksharraikanti/distsys-lab/02-kv-store"
)

// TestPooledClientRoundTrip is the pooled counterpart to Day 1's own
// TestNaiveClientRoundTrip: same Put/Append/Get correctness, this time
// through reused connections instead of a fresh dial per call.
func TestPooledClientRoundTrip(t *testing.T) {
	addrs, cleanup := tcpKVCluster(t, 3)
	defer cleanup()

	c, err := NewPooledClient(addrs, 2)
	if err != nil {
		t.Fatalf("NewPooledClient: %v", err)
	}
	defer c.Close()

	c.Put("x", "1")
	c.Append("x", "-more")
	if got := c.Get("x"); got != "1-more" {
		t.Fatalf("Get(x) = %q, want \"1-more\"", got)
	}
	if got := c.Get("nope"); got != "" {
		t.Fatalf("Get(missing key) = %q, want \"\"", got)
	}
}

// TestPoolIsActuallyFixedSize proves NewPool dials exactly size
// connections, not more: checking size connections out in a row
// (before returning any of them) must succeed immediately, and a
// (size+1)th checkout must block until one comes back — "fixed-size"
// isn't just a name, the pool really has no room to grow on demand.
func TestPoolIsActuallyFixedSize(t *testing.T) {
	addrs, cleanup := tcpKVCluster(t, 1)
	defer cleanup()

	const size = 3
	p, err := NewPool(addrs[0], size, time.Second, time.Second)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer p.Close()

	if got := len(p.free); got != size {
		t.Fatalf("len(p.free) after NewPool = %d, want %d", got, size)
	}

	checkedOut := make([]*rpc.Client, size)
	for i := 0; i < size; i++ {
		checkedOut[i] = <-p.free
	}
	if got := len(p.free); got != 0 {
		t.Fatalf("len(p.free) after checking out all %d connections = %d, want 0", size, got)
	}

	// A (size+1)th checkout must block, not error or hand out a
	// duplicate/nil connection.
	extraCh := make(chan *rpc.Client, 1)
	go func() { extraCh <- <-p.free }()

	select {
	case <-extraCh:
		t.Fatal("checkout beyond the fixed size returned immediately, want it to block until a connection is returned")
	case <-time.After(50 * time.Millisecond):
		// Expected: still blocked.
	}

	// Returning one connection must unblock the waiting checkout.
	p.free <- checkedOut[0]
	select {
	case c := <-extraCh:
		if c != checkedOut[0] {
			t.Fatal("the unblocked checkout didn't receive the connection that was just returned")
		}
	case <-time.After(time.Second):
		t.Fatal("checkout never unblocked after a connection was returned")
	}

	// Return the rest so Close (deferred above) can drain cleanly.
	for i := 1; i < size; i++ {
		p.free <- checkedOut[i]
	}
}

// TestPoolCallReturnsErrPoolExhaustedOnTimeout is Day 3's headline
// proof: with every connection held elsewhere, Call must give up and
// report ErrPoolExhausted once checkoutTimeout passes — not block
// forever (Day 2's implicit behavior) and not error out instantly
// either (a genuinely short-lived burst deserves a real chance to
// drain first).
func TestPoolCallReturnsErrPoolExhaustedOnTimeout(t *testing.T) {
	addrs, cleanup := tcpKVCluster(t, 1)
	defer cleanup()

	const checkoutTimeout = 30 * time.Millisecond
	p, err := NewPool(addrs[0], 1, checkoutTimeout, time.Second)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer p.Close()

	// Hold the only connection for the whole test — nothing ever
	// returns it, so any Call has no choice but to wait out the full
	// checkoutTimeout.
	held := <-p.free
	defer func() { p.free <- held }()

	start := time.Now()
	var reply kvstore.GetReply
	err = p.Call("KVServer.Get", &kvstore.GetArgs{Key: "x"}, &reply)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrPoolExhausted) {
		t.Fatalf("Call with the pool fully held = %v, want ErrPoolExhausted", err)
	}
	if elapsed < checkoutTimeout {
		t.Fatalf("Call returned after %s, want it to have waited at least checkoutTimeout (%s)", elapsed, checkoutTimeout)
	}
	// Generous upper bound — this is checking Call didn't give up
	// early, not pinning down exact scheduler timing.
	if elapsed > 10*checkoutTimeout {
		t.Fatalf("Call took %s, want close to checkoutTimeout (%s), not way beyond it", elapsed, checkoutTimeout)
	}
}

// TestPoolCallSucceedsIfConnectionFreesBeforeTimeout proves the OTHER
// half: a caller that only has to wait a SHORT while (well under
// checkoutTimeout) for a connection to free up must succeed normally,
// not spuriously time out just because it had to wait at all.
func TestPoolCallSucceedsIfConnectionFreesBeforeTimeout(t *testing.T) {
	addrs, cleanup := tcpKVCluster(t, 1)
	defer cleanup()

	const checkoutTimeout = time.Second
	p, err := NewPool(addrs[0], 1, checkoutTimeout, time.Second)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer p.Close()

	held := <-p.free
	go func() {
		time.Sleep(20 * time.Millisecond)
		p.free <- held
	}()

	var reply kvstore.GetReply
	if err := p.Call("KVServer.Get", &kvstore.GetArgs{Key: "x"}, &reply); err != nil {
		t.Fatalf("Call after a brief wait = %v, want it to succeed once the connection freed up", err)
	}
}

// TestPooledClientHandlesMoreConcurrentCallersThanPoolSize is Day 2's
// real correctness proof: a pool smaller than the number of concurrent
// callers must still let every caller eventually succeed — checkout
// blocking until another caller checks back in, not erroring or
// corrupting a shared connection between two concurrent callers.
func TestPooledClientHandlesMoreConcurrentCallersThanPoolSize(t *testing.T) {
	addrs, cleanup := tcpKVCluster(t, 3)
	defer cleanup()

	// Deliberately smaller than numClients below, so the pool MUST be
	// reused/contended, not just large enough that every caller always
	// finds a free connection.
	const poolSize = 2
	const numClients = 8
	const opsPerClient = 20

	// First isolate the property this test cares about most directly:
	// the Pool itself, under raw concurrent Call pressure, with no
	// retry/dedup logic layered on top to obscure whether IT is what's
	// making things work.
	pool, err := NewPool(addrs[0], poolSize, time.Second, time.Second)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer pool.Close()

	var wg sync.WaitGroup
	errs := make(chan error, numClients)
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < opsPerClient; j++ {
				var reply kvstore.GetReply
				if err := pool.Call("KVServer.Get", &kvstore.GetArgs{Key: "whatever"}, &reply); err != nil {
					errs <- fmt.Errorf("goroutine %d op %d: %v", n, j, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// Every connection must have made it back to the free list — no
	// leak, no connection stuck permanently "checked out."
	if got := len(pool.free); got != poolSize {
		t.Fatalf("len(pool.free) after all goroutines finished = %d, want %d (every connection returned)", got, poolSize)
	}

	// And the higher-level client (round-robin + retry on top of
	// pooling) still works correctly under the same kind of load. Each
	// goroutine gets its OWN PooledClient — a PooledClient is no more
	// safe for concurrent use than 02-kv-store's own Clerk is (same
	// unsynchronized seqNum/lastKnown state), so sharing one across
	// goroutines here would test something else entirely (and, as an
	// earlier version of this test found the hard way under -race,
	// corrupt it outright).
	var cwg sync.WaitGroup
	cerrs := make(chan error, numClients)
	for i := 0; i < numClients; i++ {
		cwg.Add(1)
		go func(clientNum int) {
			defer cwg.Done()
			c, err := NewPooledClient(addrs, poolSize)
			if err != nil {
				cerrs <- fmt.Errorf("client %d: NewPooledClient: %v", clientNum, err)
				return
			}
			defer c.Close()
			key := fmt.Sprintf("client-%d", clientNum)
			var expected string
			for j := 0; j < opsPerClient; j++ {
				frag := fmt.Sprintf("[%d]", j)
				c.Append(key, frag)
				expected += frag
				if got := c.Get(key); got != expected {
					cerrs <- fmt.Errorf("client %d: after %d appends, Get = %q, want %q", clientNum, j+1, got, expected)
					return
				}
			}
		}(i)
	}
	cwg.Wait()
	close(cerrs)
	for err := range cerrs {
		t.Error(err)
	}
}

// BenchmarkPooledClientPutAppend is BenchmarkNaiveClientPutAppend's
// direct counterpart — same cluster shape, same operation, same
// b.N-driven loop — so the two numbers are directly comparable. The
// finding here is itself worth recording, not just the number: pooling
// barely moves this one, because a write's latency floor is Raft's own
// replication round trip (at least one HeartbeatInterval for the entry
// to reach a majority), which dwarfs whatever a fresh TCP dial over
// loopback costs. BenchmarkPooledClientGet is where pooling actually
// shows up.
func BenchmarkPooledClientPutAppend(b *testing.B) {
	addrs, cleanup := benchTCPKVCluster(b, 3)
	defer cleanup()

	c, err := NewPooledClient(addrs, 4)
	if err != nil {
		b.Fatalf("NewPooledClient: %v", err)
	}
	defer c.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Append("bench-key", "x")
	}
}

// BenchmarkPooledClientGet is BenchmarkNaiveClientGet's direct
// counterpart. Get has no Raft-replication floor underneath it (see
// BenchmarkNaiveClientGet's doc comment), so this is the comparison
// that actually isolates what reusing connections buys — a fresh dial
// per call is a much bigger fraction of a read's total latency than of
// a write's.
func BenchmarkPooledClientGet(b *testing.B) {
	addrs, cleanup := benchTCPKVCluster(b, 3)
	defer cleanup()

	c, err := NewPooledClient(addrs, 4)
	if err != nil {
		b.Fatalf("NewPooledClient: %v", err)
	}
	defer c.Close()

	c.Put("bench-key", "x")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get("bench-key")
	}
}
