package kvstore

import (
	"fmt"
	"sync"
	"testing"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
)

// TestFullIntegrationConcurrentClientsFaultsAndSnapshotting is Day 8's
// headline test — TASKS.md's exact scenario: fault injection (crashes,
// partitions), concurrent clients, AND snapshotting all running at
// once, on the same cluster, at the same time. Every earlier day's test
// isolated one or two of these dimensions on their own (Day 5: clients
// + crashes; Day 6/7: snapshotting alone); this is the first time
// they're all live simultaneously, which is exactly the combination
// that makes InstallSnapshot (Day 7) get exercised for real rather
// than just in a hand-built scenario: a node partitioned away for long
// enough, while write traffic keeps flowing and maxRaftState keeps
// getting crossed on the majority side, comes back to find its
// nextIndex has fallen behind the leader's lastIncludedIndex — the
// exact condition only a real InstallSnapshot RPC can resolve.
//
// 5 nodes (not 3) specifically so the partition fault can split a
// genuine majority/minority, matching Stage 1 Day 12's own partition
// test shape, while still leaving 3 nodes as a functioning majority
// throughout.
//
// The invariant under test is unchanged from Day 5: every acknowledged
// write is durably visible, no acknowledged write is ever lost. Each
// client only appends to its own private key, so that check stays
// precise even with partitions and snapshots layered on top.
func TestFullIntegrationConcurrentClientsFaultsAndSnapshotting(t *testing.T) {
	const numNodes = 5
	const numClients = 6
	const opsPerClient = 40
	const maxRaftState = 500 // deliberately small: forces snapshotting well before opsPerClient*numClients writes complete

	nodes, kvs, transport, cleanup := newTestCluster(numNodes, maxRaftState)
	defer cleanup()
	waitForSingleLeader(t, nodes, 20*raft.ElectionTimeoutMax)

	faultStop := make(chan struct{})
	var faultWg sync.WaitGroup
	faultWg.Add(1)
	go func() {
		defer faultWg.Done()
		ticker := time.NewTicker(6 * raft.ElectionTimeoutMax)
		defer ticker.Stop()
		round := 0
		for {
			select {
			case <-faultStop:
				return
			case <-ticker.C:
				round++
				if round%2 == 0 {
					// Partition fault: split into a 3-2 majority/minority,
					// same shape as 01-raft's own partition test, healed
					// before the next round so the cluster spends most of
					// its time fully connected (client progress needs a
					// majority able to talk to each other, not permanent
					// chaos).
					ids := make([]int, 0, numNodes)
					for id := range nodes {
						ids = append(ids, id)
					}
					transport.Partition(ids[:3], ids[3:])
					time.Sleep(3 * raft.ElectionTimeoutMax)
					transport.Heal()
				} else {
					// Crash fault: whichever node currently believes
					// itself leader goes unreachable and comes back —
					// Day 5/Day 12's Unregister-based crash simulation.
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

	// Confirm this run actually exercised snapshotting, not just faults
	// and clients in isolation — a threshold this low, with this much
	// write volume, should have compacted well below what an
	// uncompacted log of numClients*opsPerClient entries would take.
	sawSnapshot := false
	for _, rf := range nodes {
		if len(rf.ReadSnapshot()) > 0 {
			sawSnapshot = true
			break
		}
	}
	if !sawSnapshot {
		t.Error("no node ever produced a snapshot — this run didn't actually exercise Day 6/7's snapshotting, which defeats the point of this test")
	}
}

// TestFullIntegrationSurvivesWholeClusterRestart is Day 8's other half:
// a REAL restart, not just unreachability. Every node's Raft and
// KVServer are stopped and discarded entirely, then reconstructed from
// scratch against the SAME persisters — no in-process object survives.
// This is deliberately a separate test from the one above rather than
// folded into it: KVServer's client-facing methods are called directly
// by Clerk (no RPC boundary in between, see clerk.go), so a Clerk
// holding a frozen slice of *KVServer pointers has no way to observe a
// mid-test object replacement — exactly the same reason 01-raft's own
// persistence tests (Day 11) and this package's own Day 6 restart test
// always reconstruct fresh objects and test THOSE directly, rather than
// mutating a live one out from under an existing caller. A real
// deployment's clients reconnect after a full outage anyway, so a fresh
// Clerk post-restart is realistic, not a simplification of what's
// being proven.
//
// maxRaftState is set low enough that snapshotting has definitely
// happened before the "crash," so recovery has to go through
// KVServer's ReadSnapshot-based restore path (Day 6), not just Raft's
// own log replay — proving snapshotting and restart-recovery actually
// compose at the whole-cluster level, not just the single-node level
// Day 6's own test covered.
func TestFullIntegrationSurvivesWholeClusterRestart(t *testing.T) {
	const numNodes = 3
	const maxRaftState = 400

	ids := make([]int, numNodes)
	for i := range ids {
		ids[i] = i
	}
	persisters := make(map[int]*raft.MemoryPersister, numNodes)
	for _, id := range ids {
		persisters[id] = raft.NewMemoryPersister()
	}

	buildCluster := func() (nodes map[int]*raft.Raft, kvs []*KVServer, transport *raft.FakeTransport) {
		transport = raft.NewFakeTransport()
		nodes = make(map[int]*raft.Raft, numNodes)
		kvs = make([]*KVServer, numNodes)
		for _, id := range ids {
			rf := raft.NewRaftWithPersister(id, otherPeers(ids, id), transport, persisters[id])
			nodes[id] = rf
			transport.Register(id, rf)
			kvs[id] = NewKVServer(rf, maxRaftState)
		}
		for _, rf := range nodes {
			go rf.RunElectionTimer()
			go rf.RunHeartbeats()
			go rf.RunApplyLoop()
		}
		return nodes, kvs, transport
	}
	stopCluster := func(nodes map[int]*raft.Raft, kvs []*KVServer) {
		for _, rf := range nodes {
			rf.StopElectionTimer()
		}
		for _, kv := range kvs {
			kv.Stop()
		}
	}

	nodes, kvs, _ := buildCluster()
	waitForSingleLeader(t, nodes, 20*raft.ElectionTimeoutMax)

	ck := NewClerk(kvs)
	const numKeys = 10
	const appendsPerKey = 30
	want := make(map[string]string, numKeys)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key-%d", i)
		for j := 0; j < appendsPerKey; j++ {
			frag := fmt.Sprintf("[%d]", j)
			ck.Append(key, frag)
			want[key] += frag
		}
	}
	for key, v := range want {
		if got := ck.Get(key); got != v {
			t.Fatalf("pre-restart Get(%q) = %q, want %q", key, got, v)
		}
	}

	sawSnapshotBeforeRestart := false
	preCrashCommit := 0
	for _, rf := range nodes {
		if len(rf.ReadSnapshot()) > 0 {
			sawSnapshotBeforeRestart = true
		}
		if c := rf.CommitIndex(); c > preCrashCommit {
			preCrashCommit = c
		}
	}
	if !sawSnapshotBeforeRestart {
		t.Fatal("no node had snapshotted before the simulated restart — this run wouldn't actually prove snapshot-recovery composes with restart")
	}

	// The "crash": every node's background loops stop, every object is
	// discarded. Only the persisters (standing in for each node's own
	// disk) survive.
	stopCluster(nodes, kvs)

	restartedNodes, restartedKVs, _ := buildCluster()
	defer stopCluster(restartedNodes, restartedKVs)
	waitForSingleLeader(t, restartedNodes, 20*raft.ElectionTimeoutMax)

	// A freshly elected leader's OWN commitIndex is volatile — never
	// persisted (correctly, per the Raft paper) — so it resets to (at
	// best) lastIncludedIndex on restart and has to be re-confirmed via
	// real AppendEntries replies reaching a majority again before it
	// catches back up to what was actually committed pre-crash
	// (noopLoop, Day 5, is exactly what closes this for KVServer's own
	// reads once that happens). This wait is necessary but NOT
	// sufficient on its own, which the first version of this test
	// found the hard way under extra scheduler contention: commitIndex
	// catching up doesn't mean KVServer's own store has too — that
	// still has to flow through Raft's applyPending (its own
	// HeartbeatInterval-paced tick, lagging commitIndex by design) and
	// then KVServer's applyLoop consuming ApplyCh. Waiting for
	// commitIndex narrows the window; it doesn't close it.
	waitFor(t, 20*raft.ElectionTimeoutMax, func() bool {
		for _, rf := range restartedNodes {
			if rf.CommitIndex() < preCrashCommit {
				return false
			}
		}
		return true
	})

	// So the actual verification polls for the real, fully-caught-up
	// outcome instead of trusting a single Get the instant commitIndex
	// looks right — the same "wait for the observable result, not an
	// intermediate implementation detail" approach every other
	// eventual-convergence check in this codebase already uses.
	freshCk := NewClerk(restartedKVs)
	for key, v := range want {
		waitFor(t, 20*raft.ElectionTimeoutMax, func() bool {
			return freshCk.Get(key) == v
		})
		if got := freshCk.Get(key); got != v {
			t.Fatalf("post-restart Get(%q) = %q, want %q (recovered from persisted log + snapshot)", key, got, v)
		}
	}

	// And the recovered cluster must still be able to make forward
	// progress, not just correctly answer reads of pre-restart state.
	freshCk.Put("after-restart", "still-working")
	if got := freshCk.Get("after-restart"); got != "still-working" {
		t.Fatalf(`post-restart Get("after-restart") = %q, want "still-working"`, got)
	}
}
