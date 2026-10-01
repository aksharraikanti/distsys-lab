package shardkv

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
)

// TestGroupServerSnapshotsAndCatchesUpACrashedFollower is 02-kv-store's own
// TestKVServerCatchesUpLaggingFollowerViaInstallSnapshot, retargeted at
// GroupServer: a follower is cut off long enough that the leader snapshots
// well past anything that follower ever saw, so AppendEntries alone could
// never catch it up — only Raft's InstallSnapshot can, and GroupServer's
// own applyLoop (its SnapshotValid branch) has to adopt what that RPC
// delivers correctly, store/duplicateTable/cfg/migrating/leaving all
// together, not just the Raft layer underneath it.
//
// A Move happens WHILE the follower is disconnected, deliberately — if the
// follower's cfg/leaving were simply left untouched by a broken restore
// (rather than genuinely overwritten from the snapshot), that would be
// indistinguishable from a CORRECT restore whenever nothing changed during
// the outage. Only a config transition the follower never saw directly,
// and can ONLY learn about through the snapshot, actually proves
// restoreSnapshot is doing its job rather than coincidentally matching.
func TestGroupServerSnapshotsAndCatchesUpACrashedFollower(t *testing.T) {
	_, ctrlers, _, ctrlerCleanup := newTestCtrlerCluster(3)
	defer ctrlerCleanup()
	admin := NewCtrlerClerk(ctrlers)
	admin.Join(map[int][]string{1: addrsFor(1), 2: addrsFor(2)})
	cfg := admin.Query(-1)

	const maxRaftState = 400
	nodes, g1, transport, g1Cleanup := newTestGroupCluster(3, 1, ctrlers, maxRaftState)
	defer g1Cleanup()

	leaderID := waitForGroupLeader(t, nodes, 2*time.Second)
	waitForGroupConfig(t, g1[leaderID], cfg.Num, 2*time.Second)

	laggingID := -1
	for id := range nodes {
		if id != leaderID {
			laggingID = id
			break
		}
	}

	// Two DIFFERENT shards group 1 owns: one stays with group 1 the whole
	// time (the write-heavy key forcing the snapshot), the other gets
	// explicitly Moved away while the follower is disconnected.
	key := shardOwnedByGroup(cfg, 1)
	var otherShard = -1
	for s, owner := range cfg.Shards {
		if owner == 1 && s != Key2Shard(key) {
			otherShard = s
			break
		}
	}
	if otherShard == -1 {
		t.Fatal("group 1 needs at least two distinct shards for this test to mean anything")
	}

	ck := NewShardClerk(admin, map[int][]*GroupServer{1: g1})

	// Disconnect one follower entirely, then drive enough writes through
	// the remaining majority to force the leader to snapshot well past
	// anything the disconnected node ever received.
	transport.Unregister(laggingID)
	for i := 0; i < 150; i++ {
		ck.Append(key, "-bytes-to-force-a-snapshot")
	}

	// Reassign otherShard away from group 1 while the follower can't see
	// it — this is the transition that can ONLY reach it via the snapshot.
	admin.Move(otherShard, 2)
	afterMove := admin.Query(-1)
	waitForGroupConfig(t, g1[leaderID], afterMove.Num, 3*time.Second)

	waitFor(t, 5*time.Second, func() bool {
		return len(nodes[leaderID].ReadSnapshot()) > 0
	})

	// Reconnect — the lagging node's nextIndex, from the leader's
	// point of view, now falls at or below what's already compacted
	// away. Only InstallSnapshot can resolve this.
	transport.Register(laggingID, nodes[laggingID])

	want := ck.Get(key)
	waitFor(t, 5*time.Second, func() bool {
		g1[laggingID].mu.Lock()
		defer g1[laggingID].mu.Unlock()
		return g1[laggingID].store[key] == want
	})

	// The follower must have learned the Move it never directly saw: its
	// own cfg.Num caught up to the post-Move version, and it knows
	// otherShard is now leaving (group 2 never actually runs in this test,
	// so gcLoop can never confirm readiness and clear it — it should stay
	// pending, a stable, checkable fact).
	g1[laggingID].mu.Lock()
	gotNum := g1[laggingID].cfg.Num
	gotLeavingTo, stillLeaving := g1[laggingID].leaving[otherShard]
	g1[laggingID].mu.Unlock()
	if gotNum != afterMove.Num {
		t.Fatalf("the recovered follower's cfg.Num = %d, want %d (the Move it could only have learned from the snapshot)", gotNum, afterMove.Num)
	}
	if !stillLeaving || gotLeavingTo != 2 {
		t.Fatalf("the recovered follower's leaving[%d] = (%d, present=%v), want (2, true)", otherShard, gotLeavingTo, stillLeaving)
	}

	// And the recovered node must still be a fully functional cluster
	// member afterward, not stuck in some special post-catch-up state.
	ck.Put(key, "after-catchup")
	waitFor(t, 5*time.Second, func() bool {
		g1[laggingID].mu.Lock()
		defer g1[laggingID].mu.Unlock()
		return g1[laggingID].store[key] == "after-catchup"
	})
}

// TestFullIntegrationThroughFaultsReconfigurationAndSnapshotting is Day 8's
// own headline: reconfiguration, concurrent clients on SHARED keys,
// repeated fault injection (crashing and restarting a different replica of
// a different group each round), and real snapshotting (a low
// maxRaftState), all running AT ONCE — not one after another — judged by
// Day 7's own linearizability checker instead of a simpler invariant that
// couldn't see a cross-client anomaly if faults and migration happened to
// produce one.
func TestFullIntegrationThroughFaultsReconfigurationAndSnapshotting(t *testing.T) {
	_, ctrlers, _, ctrlerCleanup := newTestCtrlerCluster(3)
	defer ctrlerCleanup()
	admin := NewCtrlerClerk(ctrlers)

	const maxRaftState = 500
	nodes1, g1, transport1, g1Cleanup := newTestGroupCluster(3, 1, ctrlers, maxRaftState)
	defer g1Cleanup()
	nodes2, g2, transport2, g2Cleanup := newTestGroupCluster(3, 2, ctrlers, maxRaftState)
	defer g2Cleanup()
	nodes3, g3, transport3, g3Cleanup := newTestGroupCluster(3, 3, ctrlers, maxRaftState)
	defer g3Cleanup()
	groups := map[int][]*GroupServer{1: g1, 2: g2, 3: g3}
	wirePeers(groups)

	admin.Join(map[int][]string{1: addrsFor(1)})
	waitForGroupConfig(t, g1[waitForGroupLeader(t, nodes1, 2*time.Second)], 1, 2*time.Second)

	const numClients = 4
	const opsPerClient = 25
	sharedKeys := []string{"shared-a", "shared-b", "shared-c"}

	history := NewHistory()
	var wg sync.WaitGroup
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ck := NewRecordingShardClerk(NewShardClerk(NewCtrlerClerk(ctrlers), groups), history, int64(i))
			for j := 0; j < opsPerClient; j++ {
				key := sharedKeys[j%len(sharedKeys)]
				switch j % 3 {
				case 0:
					ck.Put(key, fmt.Sprintf("v%d-%d", i, j))
				case 1:
					ck.Append(key, fmt.Sprintf("[%d-%d]", i, j))
				case 2:
					ck.Get(key)
				}
			}
		}(i)
	}

	// Grow the cluster and Move a couple of shards explicitly, WHILE
	// traffic is live — same shape as Day 6's own reconfiguration test.
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(3 * time.Millisecond)
		admin.Join(map[int][]string{2: addrsFor(2), 3: addrsFor(3)})
		time.Sleep(5 * time.Millisecond)
		admin.Move(Key2Shard(sharedKeys[0]), 2)
		time.Sleep(5 * time.Millisecond)
		admin.Move(Key2Shard(sharedKeys[1]), 3)
	}()

	// Repeated fault rounds across all three groups: cycle through every
	// replica id (0, 1, 2) in turn, so both leaders and followers get
	// crashed and restarted over the run, not just whichever one happens
	// to be leader at the time.
	allNodes := []map[int]*raft.Raft{nodes1, nodes2, nodes3}
	allTransports := []*raft.FakeTransport{transport1, transport2, transport3}
	var faultsFired int32
	wg.Add(1)
	go func() {
		defer wg.Done()
		const rounds = 6
		for round := 0; round < rounds; round++ {
			time.Sleep(4 * time.Millisecond)
			groupIdx := round % len(allNodes)
			victim := round % 3 // every newTestGroupCluster uses ids 0..n-1
			allTransports[groupIdx].Unregister(victim)
			atomic.AddInt32(&faultsFired, 1)
			time.Sleep(3 * time.Millisecond)
			allTransports[groupIdx].Register(victim, allNodes[groupIdx][victim])
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(45 * time.Second):
		t.Fatal("clients never finished through faults, reconfiguration, and snapshotting")
	}

	if atomic.LoadInt32(&faultsFired) == 0 {
		t.Fatal("no fault rounds actually fired")
	}

	snapshotted := false
	for _, nodes := range allNodes {
		for _, rf := range nodes {
			if len(rf.ReadSnapshot()) > 0 {
				snapshotted = true
			}
		}
	}
	if !snapshotted {
		t.Fatal("no group ever actually snapshotted during the run — maxRaftState or the op count needs raising, this test proves nothing about snapshotting otherwise")
	}

	entries := history.Entries()
	if len(entries) != numClients*opsPerClient {
		t.Fatalf("recorded %d history entries, want %d", len(entries), numClients*opsPerClient)
	}
	if !IsLinearizable(entries) {
		t.Fatal("a full run through faults, reconfiguration, and snapshotting produced a NON-linearizable history")
	}
}

// TestKnownGapStalePartitionedLeaderCanServeAStaleRead is TASKS.md's own
// closing instruction for this day: "whatever the earlier stages' known
// gaps are get their honest test here." Stage 2's Get has always carried
// one — a leader silently partitioned away doesn't know it's been
// superseded, and keeps answering from its own (now stale) store — and
// GroupServer inherited it unchanged (see group_server.go's own doc
// comments). This test doesn't fix it; it proves it's real, reproduces it
// directly, and then proves Day 7's own checker actually catches it when it
// happens — closing the loop the honest way, by demonstrating the tool
// built yesterday recognizes the exact gap this project has been
// transparent about since Stage 2, rather than only ever seeing clean runs.
func TestKnownGapStalePartitionedLeaderCanServeAStaleRead(t *testing.T) {
	_, ctrlers, _, ctrlerCleanup := newTestCtrlerCluster(3)
	defer ctrlerCleanup()
	admin := NewCtrlerClerk(ctrlers)
	admin.Join(map[int][]string{1: addrsFor(1)})
	cfg := admin.Query(-1)

	nodes, g1, transport, g1Cleanup := newTestGroupCluster(3, 1, ctrlers, -1)
	defer g1Cleanup()

	leaderID := waitForGroupLeader(t, nodes, 2*time.Second)
	waitForGroupConfig(t, g1[leaderID], cfg.Num, 2*time.Second)

	key := shardOwnedByGroup(cfg, 1)
	ck := NewShardClerk(admin, map[int][]*GroupServer{1: g1})

	putBeforeInvoke := time.Now()
	ck.Put(key, "before-partition")
	putBeforeReturn := time.Now()

	// Cut the CURRENT leader off from the rest of its own group — with
	// Partition, not Unregister. Unregister only blocks calls DIRECTED AT
	// the unregistered id; the leader's own OUTGOING AppendEntries calls
	// target the (still-registered) followers via handlerFor, which only
	// checks the RECIPIENT, so an unregistered leader can still replicate
	// completely normally. Reproducing "silently partitioned" needs BOTH
	// directions cut, which is exactly what Partition checks (reachable
	// requires caller and callee in the same group) and Unregister doesn't.
	var others []int
	for id := range nodes {
		if id != leaderID {
			others = append(others, id)
		}
	}
	transport.Partition([]int{leaderID}, others)

	// The rest of the group elects a new leader and keeps serving.
	waitFor(t, 3*time.Second, func() bool {
		for id, rf := range nodes {
			if id != leaderID && rf.State() == raft.Leader {
				return true
			}
		}
		return false
	})

	putAfterInvoke := time.Now()
	ck.Put(key, "after-partition")
	putAfterReturn := time.Now()

	// The OLD leader, still cut off, still believes itself Leader (nothing
	// on its own side of the partition can tell it otherwise), and answers
	// directly from its own last-known store — with no error at all.
	getInvoke := time.Now()
	var stale GroupGetReply
	err := g1[leaderID].Get(&GroupGetArgs{Key: key}, &stale)
	getReturn := time.Now()
	transport.Heal()

	if err != nil {
		t.Fatalf("Get RPC itself should not error: %v", err)
	}
	if stale.Err != GroupOK || stale.Value != "before-partition" {
		t.Fatalf("expected the partitioned leader to reproduce the known stale-read gap (serving %q instead of the already-committed %q), got err=%v value=%q",
			"before-partition", "after-partition", stale.Err, stale.Value)
	}

	// Day 7's checker must recognize this exact history as NOT
	// linearizable: putAfterReturn happens strictly before getInvoke, so
	// real time leaves no explanation for a Get seeing the OLDER value.
	h := []HistoryEntry{
		{ClientID: 1, Key: key, Kind: OpPut, Arg: "before-partition", Invoke: putBeforeInvoke, Return: putBeforeReturn},
		{ClientID: 2, Key: key, Kind: OpPut, Arg: "after-partition", Invoke: putAfterInvoke, Return: putAfterReturn},
		{ClientID: 3, Key: key, Kind: OpGet, Result: stale.Value, Invoke: getInvoke, Return: getReturn},
	}
	if IsLinearizable(h) {
		t.Fatal("IsLinearizable should have flagged this reproduction of the known stale-partitioned-leader gap as non-linearizable, but accepted it")
	}
}
