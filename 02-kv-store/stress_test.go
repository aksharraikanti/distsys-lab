package kvstore

import (
	"fmt"
	"sync"
	"testing"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
)

// newTestCluster wires up a fully-functional n-node cluster (real
// election timers, heartbeats, apply loops — everything Day 1-4's
// pieces need to actually work together) and returns each node's Raft
// handle, its KVServer, the shared transport (needed by tests that
// inject faults), and a cleanup func. Shared by every test below.
func newTestCluster(n int) (nodes map[int]*raft.Raft, kvs []*KVServer, transport *raft.FakeTransport, cleanup func()) {
	transport = raft.NewFakeTransport()
	ids := make([]int, n)
	for i := range ids {
		ids[i] = i
	}
	nodes = make(map[int]*raft.Raft, n)
	kvs = make([]*KVServer, n)
	for _, id := range ids {
		rf := raft.NewRaft(id, otherPeers(ids, id), transport)
		nodes[id] = rf
		transport.Register(id, rf)
		kvs[id] = NewKVServer(rf)
	}
	for _, rf := range nodes {
		go rf.RunElectionTimer()
		go rf.RunHeartbeats()
		go rf.RunApplyLoop()
	}
	cleanup = func() {
		for _, rf := range nodes {
			rf.StopElectionTimer()
		}
		for _, kv := range kvs {
			kv.Stop()
		}
	}
	return nodes, kvs, transport, cleanup
}

// TestClerkPutAppendGetRoundTrip is the Clerk's own sanity check before
// throwing concurrency or faults at it: it doesn't know which of 3 nodes
// is leader, and Put/Append/Get must still all round-trip correctly.
func TestClerkPutAppendGetRoundTrip(t *testing.T) {
	nodes, kvs, _, cleanup := newTestCluster(3)
	defer cleanup()
	waitForSingleLeader(t, nodes, 20*raft.ElectionTimeoutMax)

	ck := NewClerk(kvs)
	ck.Put("x", "1")
	ck.Append("x", "-more")
	if got := ck.Get("x"); got != "1-more" {
		t.Fatalf("Get(x) = %q, want \"1-more\"", got)
	}
	if got := ck.Get("nope"); got != "" {
		t.Fatalf("Get(missing key) = %q, want \"\"", got)
	}
}

// TestConcurrentClientsNoFaults isolates concurrency safety from fault
// tolerance: several Clerks, each its own goroutine, hammering the
// cluster at once with no faults injected. Each client only touches its
// own private key, so the check is precise — no cross-client interleave
// ordering to reason about — while still genuinely exercising concurrent
// access to the same KVServers and the same underlying Raft log.
func TestConcurrentClientsNoFaults(t *testing.T) {
	const numClients = 6
	const opsPerClient = 25

	nodes, kvs, _, cleanup := newTestCluster(3)
	defer cleanup()
	waitForSingleLeader(t, nodes, 20*raft.ElectionTimeoutMax)

	var wg sync.WaitGroup
	errs := make(chan error, numClients)
	for c := 0; c < numClients; c++ {
		wg.Add(1)
		go func(clientNum int) {
			defer wg.Done()
			ck := NewClerk(kvs)
			key := fmt.Sprintf("client-%d", clientNum)
			var expected string
			for i := 0; i < opsPerClient; i++ {
				frag := fmt.Sprintf("[%d]", i)
				ck.Append(key, frag)
				expected += frag
				if got := ck.Get(key); got != expected {
					errs <- fmt.Errorf("client %d: after %d appends, Get = %q, want %q", clientNum, i+1, got, expected)
					return
				}
			}
		}(c)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

// TestConcurrentClientsWithFaultInjection is Day 5's headline test —
// TASKS.md's exact scenario: many simulated clients hammering the
// cluster concurrently, through the same fault injection Stage 1 Day 12
// built. While client goroutines are mid-flight, a background goroutine
// repeatedly crashes whichever node currently believes itself leader —
// transport.Unregister, the same "make it truly unreachable, not just
// quiet" primitive Day 12 built, which leaves the node's own background
// loops running (so no restart bookkeeping is needed) but unreachable to
// everyone else — and reconnects it shortly after via Register.
//
// The invariant under test: every acknowledged write is durably visible,
// and no acknowledged write is ever lost. Because each client only
// appends to its own private key, "acknowledged write visible" reduces
// to "this client's own Get, after N of its own Appends, returns exactly
// the concatenation of those N fragments in order" — the Clerk's blocking
// retry loop is what makes "eventually visible despite faults" provable
// at all: if a write were silently dropped, this exact check catches it
// directly, not just eventually converges to something plausible.
func TestConcurrentClientsWithFaultInjection(t *testing.T) {
	const numClients = 4
	const opsPerClient = 15

	nodes, kvs, transport, cleanup := newTestCluster(3)
	defer cleanup()
	waitForSingleLeader(t, nodes, 20*raft.ElectionTimeoutMax)

	faultStop := make(chan struct{})
	var faultWg sync.WaitGroup
	faultWg.Add(1)
	go func() {
		defer faultWg.Done()
		ticker := time.NewTicker(5 * raft.ElectionTimeoutMax)
		defer ticker.Stop()
		for {
			select {
			case <-faultStop:
				return
			case <-ticker.C:
				for id, rf := range nodes {
					if rf.State() == raft.Leader {
						transport.Unregister(id)
						time.Sleep(2 * raft.ElectionTimeoutMax)
						transport.Register(id, rf)
						break
					}
				}
			}
		}
	}()

	var wg sync.WaitGroup
	errs := make(chan error, numClients)
	for c := 0; c < numClients; c++ {
		wg.Add(1)
		go func(clientNum int) {
			defer wg.Done()
			ck := NewClerk(kvs)
			key := fmt.Sprintf("client-%d", clientNum)
			var expected string
			for i := 0; i < opsPerClient; i++ {
				frag := fmt.Sprintf("[%d]", i)
				ck.Append(key, frag)
				expected += frag
				if got := ck.Get(key); got != expected {
					errs <- fmt.Errorf("client %d: after %d appends, Get = %q, want %q", clientNum, i+1, got, expected)
					return
				}
			}
		}(c)
	}
	wg.Wait()

	close(faultStop)
	faultWg.Wait()

	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
