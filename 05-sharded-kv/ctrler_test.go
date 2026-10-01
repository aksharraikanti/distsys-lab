package shardkv

import (
	"testing"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
)

// otherPeers is the same three-line helper 02-kv-store's own tests
// duplicate from 01-raft's — see its doc comment there for why this isn't
// exported and shared instead.
func otherPeers(ids []int, self int) []int {
	peers := make([]int, 0, len(ids)-1)
	for _, id := range ids {
		if id != self {
			peers = append(peers, id)
		}
	}
	return peers
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if !condition() {
		t.Fatalf("condition not met within %s", timeout)
	}
}

// newTestCtrlerCluster is 02-kv-store's own newTestCluster, wired to Ctrler
// instead of KVServer: real election timers, heartbeats, and apply loops,
// entirely in-process over a FakeTransport.
func newTestCtrlerCluster(n int) (nodes map[int]*raft.Raft, ctrlers []*Ctrler, transport *raft.FakeTransport, cleanup func()) {
	transport = raft.NewFakeTransport()
	ids := make([]int, n)
	for i := range ids {
		ids[i] = i
	}
	nodes = make(map[int]*raft.Raft, n)
	ctrlers = make([]*Ctrler, n)
	for _, id := range ids {
		rf := raft.NewRaft(id, otherPeers(ids, id), transport)
		nodes[id] = rf
		transport.Register(id, rf)
		ctrlers[id] = NewCtrler(rf)
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
		for _, c := range ctrlers {
			c.Stop()
		}
	}
	return nodes, ctrlers, transport, cleanup
}

// TestCtrlerQueryRefusesBeforeOwnNoopApplied is a whitebox, single-node
// reproduction of Raft §8's read rule: a node can be Leader of its term
// while still not knowing everything the CLUSTER already committed, until
// something in its own term (the no-op) has itself committed and applied.
// Built by hand (BecomeCandidate/BecomeLeader, applyLoop started
// separately) rather than through NewCtrler, specifically to freeze the
// window between "this node is Leader" and "this node's own no-op has
// applied" long enough to assert Query refuses inside it — that window is
// normally closed within one noopLoop tick, too fast to catch reliably by
// racing the real ticker.
func TestCtrlerQueryRefusesBeforeOwnNoopApplied(t *testing.T) {
	rf := raft.NewRaft(0, nil, raft.NewFakeTransport())
	if err := rf.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := rf.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	go rf.RunApplyLoop()
	defer rf.StopElectionTimer()

	c := &Ctrler{
		rf:             rf,
		configs:        []Config{{}},
		notifyChans:    make(map[int]chan ctrlerOp),
		duplicateTable: make(map[int64]int64),
		stopCh:         make(chan struct{}),
	}
	// Deliberately NOT starting c.noopLoop() — this test drives the no-op
	// itself, on its own schedule, instead of racing noopLoop's ticker.

	var before QueryReply
	if err := c.Query(&QueryArgs{Num: -1}, &before); err != nil {
		t.Fatalf("Query RPC itself should not error: %v", err)
	}
	if before.Err != CtrlerErrWrongLeader {
		t.Fatalf("Query before this leader's own no-op has applied must refuse (Raft §8), got %v", before.Err)
	}

	go c.applyLoop()
	defer c.Stop()
	if _, _, isLeader := rf.Propose(ctrlerOp{Type: "Noop"}); !isLeader {
		t.Fatal("Propose(Noop) should succeed on the leader")
	}

	waitFor(t, time.Second, func() bool {
		var after QueryReply
		return c.Query(&QueryArgs{Num: -1}, &after) == nil && after.Err == CtrlerOK
	})
}

func TestCtrlerJoinLeaveMoveAndQueryRoundTrip(t *testing.T) {
	_, ctrlers, _, cleanup := newTestCtrlerCluster(3)
	defer cleanup()
	ck := NewCtrlerClerk(ctrlers)

	zero := ck.Query(-1)
	if zero.Num != 0 || len(zero.Groups) != 0 {
		t.Fatalf("a fresh controller's config 0 should be empty, got %+v", zero)
	}

	ck.Join(map[int][]string{1: addrsFor(1), 2: addrsFor(2)})
	afterJoin := ck.Query(-1)
	if afterJoin.Num != 1 {
		t.Fatalf("after one Join, latest Num should be 1, got %d", afterJoin.Num)
	}
	if err := afterJoin.Validate(); err != nil {
		t.Fatalf("config after Join should validate: %v", err)
	}
	if len(afterJoin.Groups) != 2 {
		t.Fatalf("expected 2 groups after Join, got %d: %v", len(afterJoin.Groups), afterJoin.Groups)
	}

	ck.Join(map[int][]string{3: addrsFor(3)})
	afterSecondJoin := ck.Query(-1)
	if afterSecondJoin.Num != 2 {
		t.Fatalf("after two Joins, latest Num should be 2, got %d", afterSecondJoin.Num)
	}

	// Query(1) must still return the FIRST Join's result, not the latest —
	// history is preserved, not just the current state.
	historical := ck.Query(1)
	if historical.Num != 1 || len(historical.Groups) != 2 {
		t.Fatalf("Query(1) should return the config as of version 1, got %+v", historical)
	}

	ck.Leave([]int{2})
	afterLeave := ck.Query(-1)
	if afterLeave.Num != 3 {
		t.Fatalf("after Leave, latest Num should be 3, got %d", afterLeave.Num)
	}
	if _, stillThere := afterLeave.Groups[2]; stillThere {
		t.Fatalf("group 2 should be gone after Leave, got %v", afterLeave.Groups)
	}

	target := afterLeave.Shards[0]
	other := 1
	for gid := range afterLeave.Groups {
		if gid != target {
			other = gid
			break
		}
	}
	ck.Move(0, other)
	afterMove := ck.Query(-1)
	if afterMove.Shards[0] != other {
		t.Fatalf("Move(0, %d) should route shard 0 to group %d, got %d", other, other, afterMove.Shards[0])
	}

	// A too-large Query number falls back to the latest, per the documented
	// contract (matches MIT 6.5840's shardctrler convention).
	beyond := ck.Query(9999)
	if beyond.Num != afterMove.Num {
		t.Fatalf("Query(9999) should return the latest config (Num %d), got Num %d", afterMove.Num, beyond.Num)
	}
}

func TestCtrlerRetriedJoinAppliesOnlyOnce(t *testing.T) {
	_, ctrlers, _, cleanup := newTestCtrlerCluster(3)
	defer cleanup()

	var leader *Ctrler
	waitFor(t, 2*time.Second, func() bool {
		for _, c := range ctrlers {
			var reply QueryReply
			if err := c.Query(&QueryArgs{Num: -1}, &reply); err == nil && reply.Err == CtrlerOK {
				leader = c
				return true
			}
		}
		return false
	})

	args := &JoinArgs{Groups: map[int][]string{1: addrsFor(1)}, ClientID: 42, SeqNum: 1}
	var reply1, reply2 JoinReply
	if err := leader.Join(args, &reply1); err != nil || reply1.Err != CtrlerOK {
		t.Fatalf("first Join failed: err=%v reply=%+v", err, reply1)
	}
	// Resend the IDENTICAL ClientID+SeqNum, simulating a client that never
	// saw the first reply and retried — dedup must recognize this and NOT
	// append a second config version.
	if err := leader.Join(args, &reply2); err != nil || reply2.Err != CtrlerOK {
		t.Fatalf("retried Join failed: err=%v reply=%+v", err, reply2)
	}

	var q QueryReply
	if err := leader.Query(&QueryArgs{Num: -1}, &q); err != nil || q.Err != CtrlerOK {
		t.Fatalf("Query failed: err=%v reply=%+v", err, q)
	}
	if q.Config.Num != 1 {
		t.Fatalf("a retried Join must not append a second config version: latest Num = %d, want 1", q.Config.Num)
	}
}

func TestCtrlerMoveRejectsInvalidArgs(t *testing.T) {
	_, ctrlers, _, cleanup := newTestCtrlerCluster(3)
	defer cleanup()
	ck := NewCtrlerClerk(ctrlers)
	ck.Join(map[int][]string{1: addrsFor(1)})

	var leader *Ctrler
	waitFor(t, 2*time.Second, func() bool {
		for _, c := range ctrlers {
			var reply QueryReply
			if err := c.Query(&QueryArgs{Num: -1}, &reply); err == nil && reply.Err == CtrlerOK {
				leader = c
				return true
			}
		}
		return false
	})

	var badShard MoveReply
	if err := leader.Move(&MoveArgs{Shard: NShards, GID: 1, ClientID: 1, SeqNum: 1}, &badShard); err != nil {
		t.Fatalf("Move RPC itself should not error: %v", err)
	}
	if badShard.Err != CtrlerErrInvalidArgs {
		t.Fatalf("Move with an out-of-range shard should be rejected, got %v", badShard.Err)
	}

	var badGID MoveReply
	if err := leader.Move(&MoveArgs{Shard: 0, GID: 999, ClientID: 1, SeqNum: 2}, &badGID); err != nil {
		t.Fatalf("Move RPC itself should not error: %v", err)
	}
	if badGID.Err != CtrlerErrInvalidArgs {
		t.Fatalf("Move to an unknown group should be rejected, got %v", badGID.Err)
	}

	latest := ck.Query(-1)
	if latest.Num != 1 {
		t.Fatalf("rejected Moves must never be proposed: latest Num = %d, want 1 (only the earlier Join)", latest.Num)
	}
}

// TestCtrlerSurvivesLeaderChange proves the machinery TASKS.md asks this
// day to prove is really reusable: cutting off the current leader mid-flight
// (the same fault 02-kv-store's leader_change_test.go and Stage 3's health
// tests use) must not corrupt or lose a Join — either it commits before the
// cutoff and a later Query sees it, or the client's retry lands on the new
// leader and it commits there. Either way, exactly one new config version
// results, never zero, never two.
func TestCtrlerSurvivesLeaderChange(t *testing.T) {
	nodes, ctrlers, transport, cleanup := newTestCtrlerCluster(3)
	defer cleanup()
	ck := NewCtrlerClerk(ctrlers)

	var leaderID = -1
	waitFor(t, 2*time.Second, func() bool {
		for id, rf := range nodes {
			if rf.State() == raft.Leader {
				leaderID = id
				return true
			}
		}
		return false
	})

	done := make(chan struct{})
	go func() {
		ck.Join(map[int][]string{7: addrsFor(7)})
		close(done)
	}()

	// Cut the leader off from the rest of the cluster shortly after
	// proposing — win or lose the race with commit, the Clerk must still
	// converge, either via this leader's own commit or a retry against
	// whoever wins the resulting election. This has to be Partition, not
	// Unregister: Unregister only blocks calls DIRECTED AT the unregistered
	// id, so the leader's own outgoing AppendEntries to its (still
	// registered) followers keep going through unaffected and it just
	// keeps committing normally — never forcing the "lose the race" branch
	// this test claims to exercise. Partition blocks both directions.
	var others []int
	for id := range nodes {
		if id != leaderID {
			others = append(others, id)
		}
	}
	time.Sleep(2 * time.Millisecond)
	transport.Partition([]int{leaderID}, others)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Join never completed after a leader cutoff")
	}
	transport.Heal()

	latest := ck.Query(-1)
	if _, ok := latest.Groups[7]; !ok {
		t.Fatalf("group 7 should be present after Join survives the leader cutoff, got %v", latest.Groups)
	}
	if latest.Num != 1 {
		t.Fatalf("exactly one Join should have committed exactly once: latest Num = %d, want 1", latest.Num)
	}
}
