package pool

import (
	"net"
	"testing"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
	kvstore "github.com/aksharraikanti/distsys-lab/02-kv-store"
)

// singleNodeKVServer builds a single-node Raft/KVServer pair already
// promoted to leader, exposed over real TCP via ServeKVServer at addr
// (":0" for an OS-assigned port). Returns the actual address, the
// listener (so a test can close it to simulate the node going away and
// later re-Serve on the SAME address to simulate it coming back), and
// a cleanup func for the Raft/KVServer side.
func singleNodeKVServer(t *testing.T, addr string) (actualAddr string, l net.Listener, cleanup func()) {
	t.Helper()
	rf := raft.NewRaft(0, nil, raft.NewFakeTransport())
	if err := rf.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := rf.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	go rf.RunApplyLoop()
	kv := kvstore.NewKVServer(rf, -1)

	l, err := ServeKVServer(addr, kv)
	if err != nil {
		t.Fatalf("ServeKVServer: %v", err)
	}
	cleanup = func() {
		rf.StopElectionTimer()
		kv.Stop()
	}
	return l.Addr().String(), l, cleanup
}

// TestPoolEvictsBrokenConnectionAndReplacesIt proves the headline
// behavior directly: a connection that's gone bad (here, simulated by
// closing it out from under the pool — exactly what a real dropped
// connection looks like from the pool's point of view, regardless of
// WHY it dropped) is detected on next use, evicted, and transparently
// replaced — the CALL that discovered the break still reports the
// error (never silently swallowed), but the very next call succeeds
// using the freshly dialed replacement, without the caller having to
// do anything special.
func TestPoolEvictsBrokenConnectionAndReplacesIt(t *testing.T) {
	addr, l, cleanup := singleNodeKVServer(t, "127.0.0.1:0")
	defer l.Close()
	defer cleanup()

	opts := testPoolOptions(1)
	opts.RedialInterval = 10 * time.Millisecond
	p, err := NewPool(addr, opts)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer p.Close()

	// Simulate the connection having gone bad: close it out from under
	// the pool directly (white-box — same package), then put it back,
	// the same shape a real broken connection sitting in the free list
	// would have.
	broken := <-p.free
	broken.client.Close()
	p.free <- broken

	var reply kvstore.GetReply
	if err := p.Call("KVServer.Get", &kvstore.GetArgs{Key: "x"}, &reply); err == nil {
		t.Fatal("Call using a connection that was closed out from under the pool: want an error, got nil")
	}

	// The pool must have evicted it and dialed a fresh replacement —
	// the NEXT call should succeed normally.
	if err := p.Call("KVServer.Get", &kvstore.GetArgs{Key: "x"}, &reply); err != nil {
		t.Fatalf("Call after the broken connection was evicted: want success, got %v", err)
	}
}

// TestPoolRecoversAfterNodeBecomesUnreachableThenComesBack is Day 4's
// real payoff, matching its own TASKS.md description directly: the KV
// node behind a pool crashes or restarts. The pool must survive that —
// failing calls made against it while it's down, then transparently
// recovering, with no caller intervention, once it's reachable again.
func TestPoolRecoversAfterNodeBecomesUnreachableThenComesBack(t *testing.T) {
	addr, l, cleanup := singleNodeKVServer(t, "127.0.0.1:0")
	defer cleanup()

	const redialInterval = 15 * time.Millisecond
	opts := testPoolOptions(1)
	opts.RedialInterval = redialInterval
	p, err := NewPool(addr, opts)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}
	defer p.Close()

	var reply kvstore.GetReply
	if err := p.Call("KVServer.Get", &kvstore.GetArgs{Key: "x"}, &reply); err != nil {
		t.Fatalf("initial Call: %v", err)
	}

	// "Crash" the node: stop accepting new connections (so redial fails
	// once the pool tries) AND break the pool's existing connection
	// directly. Closing the listener alone would NOT affect an
	// already-established connection — net/rpc keeps serving accepted
	// connections independently of the listener that accepted them —
	// so simulating the break client-side is what actually exercises
	// "this connection just failed," which is the observable symptom
	// a real crash produces regardless of the underlying cause.
	l.Close()
	broken := <-p.free
	broken.client.Close()
	p.free <- broken

	if err := p.Call("KVServer.Get", &kvstore.GetArgs{Key: "x"}, &reply); err == nil {
		t.Fatal("Call while the node is down: want an error, got nil")
	}

	// "Restart" the node on the EXACT same address.
	_, l2, cleanup2 := singleNodeKVServer(t, addr)
	defer cleanup2()
	defer l2.Close()

	// The pool's background redial should recover on its own — no
	// caller needs to retry Call in a loop for the POOL itself to heal;
	// once it has, an ordinary Call succeeds again.
	waitFor(t, 20*redialInterval, func() bool {
		return p.Call("KVServer.Get", &kvstore.GetArgs{Key: "x"}, &reply) == nil
	})
}
