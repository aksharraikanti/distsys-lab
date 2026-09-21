package pool

import (
	"fmt"
	"net/rpc"
	"sync"
	"testing"
	"time"

	kvstore "github.com/aksharraikanti/distsys-lab/02-kv-store"
)

// elasticTestOptions returns PoolOptions for a pool that can actually
// grow and shrink (unlike testPoolOptions' fixed MinSize==MaxSize),
// with an IdleTimeout short enough that shrink-focused tests don't
// have to wait long to observe it.
func elasticTestOptions(minSize, maxSize int, idleTimeout time.Duration) PoolOptions {
	return PoolOptions{
		MinSize:         minSize,
		MaxSize:         maxSize,
		CheckoutTimeout: time.Second,
		RedialInterval:  time.Second,
		IdleTimeout:     idleTimeout,
	}
}

// TestPoolGrowsOnDemandUpToMaxSize proves the pool dials NEW
// connections in response to real concurrent demand, past MinSize, up
// to MaxSize — not just handing out MinSize connections and making
// everyone else wait behind them the way Day 2-4's fixed-size pool
// did.
func TestPoolGrowsOnDemandUpToMaxSize(t *testing.T) {
	addr, l, cleanup := singleNodeKVServer(t, "127.0.0.1:0")
	defer l.Close()
	defer cleanup()

	p, err := NewPool(addr, elasticTestOptions(1, 5, time.Hour))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer p.Close()

	if got := poolCount(p); got != 1 {
		t.Fatalf("count right after construction = %d, want MinSize (1)", got)
	}

	// Check out 5 connections in a row without returning any of them —
	// more than MinSize, exactly MaxSize. Every one of these must
	// succeed without blocking on checkoutTimeout, since there's room
	// to grow the whole way.
	conns := make([]*rpc.Client, 5)
	for i := range conns {
		c, err := p.checkout()
		if err != nil {
			t.Fatalf("checkout %d/5: %v", i+1, err)
		}
		conns[i] = c
	}
	if got := poolCount(p); got != 5 {
		t.Fatalf("count after checking out 5 (MaxSize) = %d, want 5", got)
	}

	// A 6th checkout, with the pool already at MaxSize and nothing
	// free, must NOT grow further — it has to wait, exactly like Day
	// 3's own exhaustion behavior.
	done := make(chan error, 1)
	go func() {
		_, err := p.checkout()
		done <- err
	}()
	select {
	case <-done:
		t.Fatal("checkout beyond MaxSize returned immediately, want it to block")
	case <-time.After(50 * time.Millisecond):
		// Expected: still blocked, waiting for one of the 5 to free up.
	}

	p.checkin(conns[0])
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("checkout after a connection freed up: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("checkout never unblocked after a connection was checked back in")
	}

	for _, c := range conns[1:] {
		p.checkin(c)
	}
}

// TestPoolShrinksIdleConnectionsDownToMinSize proves the other half:
// connections grown past MinSize get closed once they've sat idle past
// IdleTimeout — reuse-vs-cold-start cuts both ways, and this stage
// exists specifically because holding idle connections open forever
// wastes resources on both ends.
func TestPoolShrinksIdleConnectionsDownToMinSize(t *testing.T) {
	addr, l, cleanup := singleNodeKVServer(t, "127.0.0.1:0")
	defer l.Close()
	defer cleanup()

	const idleTimeout = 30 * time.Millisecond
	p, err := NewPool(addr, elasticTestOptions(1, 5, idleTimeout))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer p.Close()

	// Grow to MaxSize, then return everything — 5 idle connections,
	// only 1 (MinSize) of which should survive past idleTimeout.
	conns := make([]*rpc.Client, 5)
	for i := range conns {
		c, err := p.checkout()
		if err != nil {
			t.Fatalf("checkout %d/5: %v", i+1, err)
		}
		conns[i] = c
	}
	for _, c := range conns {
		p.checkin(c)
	}
	if got := poolCount(p); got != 5 {
		t.Fatalf("count right after returning all 5 = %d, want 5", got)
	}

	waitFor(t, 20*idleTimeout, func() bool {
		return poolCount(p) == 1
	})
	if got := len(p.free); got != 1 {
		t.Fatalf("len(p.free) after shrinking = %d, want 1 (MinSize, still usable)", got)
	}

	// The one surviving connection must still actually work.
	var reply kvstore.GetReply
	if err := p.Call("KVServer.Get", &kvstore.GetArgs{Key: "x"}, &reply); err != nil {
		t.Fatalf("Call after shrinking back to MinSize: %v", err)
	}
}

// TestPoolLoadIdleLoadCycle is Day 5's own named scenario, end to end,
// through real Call traffic rather than raw checkout/checkin: growth
// under real concurrent load, shrinkage once that load stops and
// connections sit idle, and regrowth once real load resumes — proving
// the pool doesn't just grow once and get stuck large, or shrink once
// and refuse to grow back.
func TestPoolLoadIdleLoadCycle(t *testing.T) {
	addr, l, cleanup := singleNodeKVServer(t, "127.0.0.1:0")
	defer l.Close()
	defer cleanup()

	const idleTimeout = 30 * time.Millisecond
	p, err := NewPool(addr, elasticTestOptions(1, 6, idleTimeout))
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer p.Close()

	load := func(concurrency int) {
		var wg sync.WaitGroup
		errs := make(chan error, concurrency)
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				var reply kvstore.PutAppendReply
				args := &kvstore.PutAppendArgs{Key: "x", Value: "v", Op: "Append", ClientID: 1, SeqNum: int64(n + 1)}
				if err := p.Call("KVServer.PutAppend", args, &reply); err != nil {
					errs <- fmt.Errorf("goroutine %d: %v", n, err)
				}
			}(i)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
	}

	if got := poolCount(p); got != 1 {
		t.Fatalf("count before any load = %d, want MinSize (1)", got)
	}

	// Load: 6 concurrent callers should grow the pool up toward MaxSize.
	load(6)
	afterFirstLoad := poolCount(p)
	if afterFirstLoad <= 1 {
		t.Fatalf("count after 6 concurrent calls = %d, want > 1 (MinSize) — the pool should have grown", afterFirstLoad)
	}

	// Idle: wait past idleTimeout with no traffic at all.
	waitFor(t, 20*idleTimeout, func() bool {
		return poolCount(p) == 1
	})

	// Load again: the pool must grow back, not stay stuck at MinSize
	// just because it shrank once already.
	load(6)
	afterSecondLoad := poolCount(p)
	if afterSecondLoad <= 1 {
		t.Fatalf("count after a second round of 6 concurrent calls = %d, want > 1 — the pool should have regrown after shrinking", afterSecondLoad)
	}
}

func poolCount(p *Pool) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.count
}
