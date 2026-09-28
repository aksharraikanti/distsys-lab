package shardkv

import (
	"fmt"
	"testing"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
)

// newTestGroupCluster builds an n-node Raft cluster of GroupServers for
// group gid, each replica polling its OWN CtrlerClerk against ctrlers —
// CtrlerClerk is explicitly not safe for concurrent use (see its doc
// comment), and every replica's configPollLoop can call Query the moment it
// becomes leader, so sharing one Clerk across replicas (or across groups)
// would violate that contract even though it would often happen to work.
func newTestGroupCluster(n, gid int, ctrlers []*Ctrler) (nodes map[int]*raft.Raft, servers []*GroupServer, transport *raft.FakeTransport, cleanup func()) {
	transport = raft.NewFakeTransport()
	ids := make([]int, n)
	for i := range ids {
		ids[i] = i
	}
	nodes = make(map[int]*raft.Raft, n)
	servers = make([]*GroupServer, n)
	for _, id := range ids {
		rf := raft.NewRaft(id, otherPeers(ids, id), transport)
		nodes[id] = rf
		transport.Register(id, rf)
		servers[id] = NewGroupServer(rf, gid, NewCtrlerClerk(ctrlers))
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
		for _, s := range servers {
			s.Stop()
		}
	}
	return nodes, servers, transport, cleanup
}

// groupNodes exposes a GroupServer cluster's own raft.Raft handles, so
// waitForGroupLeader (which just wants a map[int]*raft.Raft) works for a
// group cluster the same way it works for newTestGroupCluster's own
// returned nodes map.
func groupNodes(servers []*GroupServer) map[int]*raft.Raft {
	nodes := make(map[int]*raft.Raft, len(servers))
	for i, s := range servers {
		nodes[i] = s.rf
	}
	return nodes
}

// waitForGroupLeader polls until exactly one node in nodes is Leader and
// returns its id — 02-kv-store's own waitForSingleLeader, duplicated for
// the same reason otherPeers is: it's an unexported test helper in another
// package.
func waitForGroupLeader(t *testing.T, nodes map[int]*raft.Raft, timeout time.Duration) int {
	t.Helper()
	leaderID := -1
	waitFor(t, timeout, func() bool {
		leaders := 0
		for id, rf := range nodes {
			if rf.State() == raft.Leader {
				leaders++
				leaderID = id
			}
		}
		return leaders == 1
	})
	return leaderID
}

// waitForGroupConfig blocks until s has itself APPLIED config version num —
// not just until the controller knows about it. Directly reading s.cfg
// (whitebox, same package) rather than polling Get/PutAppend, since before
// s owns anything at all, there may be no key it will answer for either way.
func waitForGroupConfig(t *testing.T, s *GroupServer, num int, timeout time.Duration) {
	t.Helper()
	waitFor(t, timeout, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.cfg.Num >= num
	})
}

// shardOwnedByGroup returns some key whose shard cfg assigns to gid, by
// searching forward from Key2Shard rather than reasoning about the hash
// backwards. Panics if gid owns no shard in cfg — every test using this
// controls cfg itself, so that would be a test bug, not a runtime input.
func shardOwnedByGroup(cfg Config, gid int) string {
	for s, owner := range cfg.Shards {
		if owner != gid {
			continue
		}
		for i := 0; ; i++ {
			key := fmt.Sprintf("probe-%d", i)
			if Key2Shard(key) == s {
				return key
			}
		}
	}
	panic("shardOwnedByGroup: gid owns no shard in this config")
}

func TestGroupServerRoundTripAndCrossGroupRefusal(t *testing.T) {
	_, ctrlers, _, ctrlerCleanup := newTestCtrlerCluster(3)
	defer ctrlerCleanup()
	admin := NewCtrlerClerk(ctrlers)
	admin.Join(map[int][]string{1: addrsFor(1), 2: addrsFor(2)})
	cfg := admin.Query(-1)

	_, g1, _, g1Cleanup := newTestGroupCluster(3, 1, ctrlers)
	defer g1Cleanup()
	_, g2, _, g2Cleanup := newTestGroupCluster(3, 2, ctrlers)
	defer g2Cleanup()

	ck := NewShardClerk(admin, map[int][]*GroupServer{1: g1, 2: g2})

	keyG1 := shardOwnedByGroup(cfg, 1)
	keyG2 := shardOwnedByGroup(cfg, 2)

	ck.Put(keyG1, "v1")
	ck.Put(keyG2, "v2")
	if got := ck.Get(keyG1); got != "v1" {
		t.Fatalf("Get(%q) = %q, want %q", keyG1, got, "v1")
	}
	if got := ck.Get(keyG2); got != "v2" {
		t.Fatalf("Get(%q) = %q, want %q", keyG2, got, "v2")
	}

	// Direct proof, bypassing ShardClerk's own retry: group 2's leader must
	// refuse a key that belongs to group 1's shard.
	g2LeaderID := waitForGroupLeader(t, groupNodes(g2), 2*time.Second)
	waitForGroupConfig(t, g2[g2LeaderID], cfg.Num, 2*time.Second)
	var reply GroupGetReply
	if err := g2[g2LeaderID].Get(&GroupGetArgs{Key: keyG1}, &reply); err != nil {
		t.Fatalf("Get RPC itself should not error: %v", err)
	}
	if reply.Err != GroupErrWrongGroup {
		t.Fatalf("group 2 should refuse a key belonging to group 1's shard, got %v", reply.Err)
	}
}

func TestGroupServerRetriedPutAppendAppliesOnlyOnce(t *testing.T) {
	_, ctrlers, _, ctrlerCleanup := newTestCtrlerCluster(3)
	defer ctrlerCleanup()
	admin := NewCtrlerClerk(ctrlers)
	admin.Join(map[int][]string{1: addrsFor(1)})
	cfg := admin.Query(-1)

	nodes, g1, _, g1Cleanup := newTestGroupCluster(3, 1, ctrlers)
	defer g1Cleanup()
	leaderID := waitForGroupLeader(t, nodes, 2*time.Second)
	leader := g1[leaderID]
	waitForGroupConfig(t, leader, cfg.Num, 2*time.Second)

	key := shardOwnedByGroup(cfg, 1)
	args := &GroupPutAppendArgs{Key: key, Value: "x", Op: "Append", ClientID: 7, SeqNum: 1}
	var r1, r2 GroupPutAppendReply
	if err := leader.PutAppend(args, &r1); err != nil || r1.Err != GroupOK {
		t.Fatalf("first Append failed: err=%v reply=%+v", err, r1)
	}
	if err := leader.PutAppend(args, &r2); err != nil || r2.Err != GroupOK {
		t.Fatalf("retried Append failed: err=%v reply=%+v", err, r2)
	}

	var g GroupGetReply
	if err := leader.Get(&GroupGetArgs{Key: key}, &g); err != nil || g.Err != GroupOK {
		t.Fatalf("Get failed: err=%v reply=%+v", err, g)
	}
	if g.Value != "x" {
		t.Fatalf("a retried Append must not apply twice: value = %q, want %q", g.Value, "x")
	}
}

// TestGroupServerMoveReroutesOwnershipAndShardClerkFollows proves the
// end-to-end loop TASKS.md describes: once the controller reassigns a
// shard, the new owner starts serving it and the OLD owner starts refusing
// it, purely from each group's own poll loop picking up the change — and
// ShardClerk, given no help beyond "the wrong group said no," transparently
// refetches the config and finds the new owner on its own. The moved shard
// is never written before the Move, so there's no data to lose — real
// migration is Day 5's job; this day only proves the routing/refusal.
func TestGroupServerMoveReroutesOwnershipAndShardClerkFollows(t *testing.T) {
	_, ctrlers, _, ctrlerCleanup := newTestCtrlerCluster(3)
	defer ctrlerCleanup()
	admin := NewCtrlerClerk(ctrlers)
	admin.Join(map[int][]string{1: addrsFor(1), 2: addrsFor(2)})
	cfg := admin.Query(-1)

	nodes1, g1, _, g1Cleanup := newTestGroupCluster(3, 1, ctrlers)
	defer g1Cleanup()
	nodes2, g2, _, g2Cleanup := newTestGroupCluster(3, 2, ctrlers)
	defer g2Cleanup()

	ck := NewShardClerk(admin, map[int][]*GroupServer{1: g1, 2: g2})

	key := shardOwnedByGroup(cfg, 1)
	shard := Key2Shard(key)

	waitForGroupConfig(t, g1[waitForGroupLeader(t, nodes1, 2*time.Second)], cfg.Num, 2*time.Second)
	waitForGroupConfig(t, g2[waitForGroupLeader(t, nodes2, 2*time.Second)], cfg.Num, 2*time.Second)

	admin.Move(shard, 2)
	afterMove := admin.Query(-1)

	waitForGroupConfig(t, g1[waitForGroupLeader(t, nodes1, 2*time.Second)], afterMove.Num, 3*time.Second)
	waitForGroupConfig(t, g2[waitForGroupLeader(t, nodes2, 2*time.Second)], afterMove.Num, 3*time.Second)

	// ShardClerk still has the OLD config cached; it must transparently
	// refetch on GroupErrWrongGroup and find group 2. Bounded, not a bare
	// call, so a real routing bug fails the test instead of hanging it.
	done := make(chan string, 1)
	go func() {
		ck.Put(key, "moved")
		done <- ck.Get(key)
	}()
	select {
	case got := <-done:
		if got != "moved" {
			t.Fatalf("Get(%q) after Move = %q, want %q", key, got, "moved")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ShardClerk never converged on the new owner after Move")
	}

	g1LeaderID := waitForGroupLeader(t, nodes1, 2*time.Second)
	var reply GroupGetReply
	if err := g1[g1LeaderID].Get(&GroupGetArgs{Key: key}, &reply); err != nil {
		t.Fatalf("Get RPC itself should not error: %v", err)
	}
	if reply.Err != GroupErrWrongGroup {
		t.Fatalf("group 1 (old owner) should refuse the moved shard, got %v", reply.Err)
	}
}

// TestGroupServerWriteSurvivesLeaderCutoff is Ctrler's own leader-cutoff
// test, retargeted at a GroupServer's PutAppend: cutting the leader off
// mid-flight must not lose the write or duplicate it, whether it commits
// just before the cutoff or the client's retry lands on the new leader.
func TestGroupServerWriteSurvivesLeaderCutoff(t *testing.T) {
	_, ctrlers, _, ctrlerCleanup := newTestCtrlerCluster(3)
	defer ctrlerCleanup()
	admin := NewCtrlerClerk(ctrlers)
	admin.Join(map[int][]string{1: addrsFor(1)})
	cfg := admin.Query(-1)

	nodes, g1, transport, g1Cleanup := newTestGroupCluster(3, 1, ctrlers)
	defer g1Cleanup()
	leaderID := waitForGroupLeader(t, nodes, 2*time.Second)
	waitForGroupConfig(t, g1[leaderID], cfg.Num, 2*time.Second)

	ck := NewShardClerk(admin, map[int][]*GroupServer{1: g1})
	key := shardOwnedByGroup(cfg, 1)

	done := make(chan struct{})
	go func() {
		ck.Put(key, "durable")
		close(done)
	}()

	time.Sleep(2 * time.Millisecond)
	transport.Unregister(leaderID)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Put never completed after a leader cutoff")
	}
	transport.Register(leaderID, nodes[leaderID])

	if got := ck.Get(key); got != "durable" {
		t.Fatalf("Get(%q) after leader cutoff = %q, want %q", key, got, "durable")
	}
}

// TestGroupServerGetRefusesBeforeOwnNoopApplied is Ctrler's own whitebox
// no-op-gate test (see its doc comment for the full reasoning), retargeted
// at GroupServer.Get. Without this, mutation testing showed the exact same
// gap it showed for Ctrler: nothing in the rest of this file's coverage
// forces the window between "elected leader" and "own no-op applied" open
// long enough to observe a missing gate.
func TestGroupServerGetRefusesBeforeOwnNoopApplied(t *testing.T) {
	_, ctrlers, _, ctrlerCleanup := newTestCtrlerCluster(3)
	defer ctrlerCleanup()
	admin := NewCtrlerClerk(ctrlers)
	admin.Join(map[int][]string{1: addrsFor(1)})
	cfg := admin.Query(-1)
	key := shardOwnedByGroup(cfg, 1)

	rf := raft.NewRaft(0, nil, raft.NewFakeTransport())
	if err := rf.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := rf.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	go rf.RunApplyLoop()
	defer rf.StopElectionTimer()

	s := &GroupServer{
		rf:             rf,
		gid:            1,
		ctrl:           NewCtrlerClerk(ctrlers),
		store:          make(map[string]string),
		notifyChans:    make(map[int]chan groupApplyResult),
		duplicateTable: make(map[int64]int64),
		stopCh:         make(chan struct{}),
	}
	// Deliberately not starting s.noopLoop() — driving the no-op by hand,
	// same reason as Ctrler's own version of this test.
	go s.applyLoop()
	defer s.Stop()

	// Adopt ownership of the shard directly (bypassing configPollLoop) so
	// the ONLY thing standing between "leader" and "answers Get" is the
	// no-op gate this test exists to check.
	if _, _, isLeader := rf.Propose(groupOp{Type: "Config", Config: cfg}); !isLeader {
		t.Fatal("Propose(Config) should succeed on the leader")
	}
	waitForGroupConfig(t, s, cfg.Num, time.Second)

	var before GroupGetReply
	if err := s.Get(&GroupGetArgs{Key: key}, &before); err != nil {
		t.Fatalf("Get RPC itself should not error: %v", err)
	}
	if before.Err != GroupErrWrongLeader {
		t.Fatalf("Get before this leader's own no-op has applied must refuse (Raft §8), got %v", before.Err)
	}

	if _, _, isLeader := rf.Propose(groupOp{Type: "Noop"}); !isLeader {
		t.Fatal("Propose(Noop) should succeed on the leader")
	}
	waitFor(t, time.Second, func() bool {
		var after GroupGetReply
		return s.Get(&GroupGetArgs{Key: key}, &after) == nil && after.Err == GroupErrNoKey
	})
}

// TestGroupServerRejectsWriteThatArrivesAfterConfigRevokesOwnership targets
// the check PutAppend's own pre-check CANNOT stand in for: applyLoop's
// ownership check at the moment a Put/Append entry actually applies, not
// whenever it was proposed or pre-checked. Every other test in this file
// routes writes through PutAppend, whose pre-check rejects an obviously
// wrong-group write before it's ever proposed — which means the apply-time
// check was never actually exercised by any of them; mutating it away
// (`if !s.ownsLocked(...)` -> `if false`) passed the whole suite. This test
// bypasses PutAppend entirely and proposes directly, ordering a Config entry
// that revokes ownership BEFORE the Put entry in the log — reproducing
// exactly the race PutAppend's pre-check cannot close (ownership changing
// in the gap between a client's check and its entry's actual commit) on a
// single always-leader node, deterministically, with no election timing
// needed.
func TestGroupServerRejectsWriteThatArrivesAfterConfigRevokesOwnership(t *testing.T) {
	_, ctrlers, _, ctrlerCleanup := newTestCtrlerCluster(3)
	defer ctrlerCleanup()

	rf := raft.NewRaft(0, nil, raft.NewFakeTransport())
	if err := rf.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := rf.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	go rf.RunApplyLoop()
	defer rf.StopElectionTimer()

	s := &GroupServer{
		rf:             rf,
		gid:            1,
		ctrl:           NewCtrlerClerk(ctrlers),
		store:          make(map[string]string),
		notifyChans:    make(map[int]chan groupApplyResult),
		duplicateTable: make(map[int64]int64),
		stopCh:         make(chan struct{}),
	}
	go s.applyLoop()
	defer s.Stop()

	allMine := Config{Num: 1, Groups: map[int][]string{1: addrsFor(1), 2: addrsFor(2)}}
	for i := range allMine.Shards {
		allMine.Shards[i] = 1
	}
	if _, _, isLeader := rf.Propose(groupOp{Type: "Config", Config: allMine}); !isLeader {
		t.Fatal("Propose(Config) should succeed on the leader")
	}
	if _, _, isLeader := rf.Propose(groupOp{Type: "Noop"}); !isLeader {
		t.Fatal("Propose(Noop) should succeed on the leader")
	}
	waitForGroupConfig(t, s, allMine.Num, time.Second)

	key := "k"
	shard := Key2Shard(key)
	revoked := allMine
	revoked.Num = 2
	revoked.Shards[shard] = 2

	// Propose the ownership-revoking Config, THEN the Put — same leader, so
	// they land in this exact order in the log, and apply in this exact
	// order too. proposeAndWait blocks until the Put resolves, so by the
	// time it returns, both entries have already applied.
	if _, _, isLeader := rf.Propose(groupOp{Type: "Config", Config: revoked}); !isLeader {
		t.Fatal("Propose(Config) should succeed on the leader")
	}
	putErr := s.proposeAndWait(groupOp{Type: "Put", Key: key, Value: "v", ClientID: 99, SeqNum: 1})
	if putErr != GroupErrWrongGroup {
		t.Fatalf("a Put whose shard was revoked before it applied must be rejected, got %v", putErr)
	}

	// Prove the store itself was never touched (not just that Get refuses
	// to serve it): restore ownership and check the key is still unset,
	// not silently holding the rejected write's value.
	restored := revoked
	restored.Num = 3
	restored.Shards[shard] = 1
	if _, _, isLeader := rf.Propose(groupOp{Type: "Config", Config: restored}); !isLeader {
		t.Fatal("Propose(Config) should succeed on the leader")
	}
	waitForGroupConfig(t, s, restored.Num, time.Second)

	var reply GroupGetReply
	if err := s.Get(&GroupGetArgs{Key: key}, &reply); err != nil {
		t.Fatalf("Get RPC itself should not error: %v", err)
	}
	if reply.Err != GroupErrNoKey {
		t.Fatalf("the rejected Put must never have mutated the store: Get returned %v (value %q), want ErrNoKey", reply.Err, reply.Value)
	}
}

// TestGroupServerRejectsOutOfOrderConfig checks the OTHER half of
// applyLoop's Config guard: op.Config.Num == s.cfg.Num+1, not merely
// op.Config.Num > s.cfg.Num. A greater-than check would still reject stale
// duplicates, but it would also silently ACCEPT a config that skips a
// version — which configPollLoop's own discipline (always request exactly
// cfg.Num+1) never triggers by itself, so nothing else in this file
// exercises the difference. It matters for Day 5: a skipped version's
// transition never gets a chance to trigger whatever migration it should
// have, so "exactly next" has to be enforced now, not discovered missing
// once migration depends on it.
func TestGroupServerRejectsOutOfOrderConfig(t *testing.T) {
	_, ctrlers, _, ctrlerCleanup := newTestCtrlerCluster(3)
	defer ctrlerCleanup()

	rf := raft.NewRaft(0, nil, raft.NewFakeTransport())
	if err := rf.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := rf.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	go rf.RunApplyLoop()
	defer rf.StopElectionTimer()

	s := &GroupServer{
		rf:             rf,
		gid:            1,
		ctrl:           NewCtrlerClerk(ctrlers),
		store:          make(map[string]string),
		notifyChans:    make(map[int]chan groupApplyResult),
		duplicateTable: make(map[int64]int64),
		stopCh:         make(chan struct{}),
	}
	go s.applyLoop()
	defer s.Stop()

	cfg1 := Config{Num: 1, Groups: map[int][]string{1: addrsFor(1)}}
	if _, _, isLeader := rf.Propose(groupOp{Type: "Config", Config: cfg1}); !isLeader {
		t.Fatal("Propose(Config) should succeed on the leader")
	}
	waitForGroupConfig(t, s, 1, time.Second)

	cfg3 := Config{Num: 3, Groups: map[int][]string{1: addrsFor(1)}}
	if _, _, isLeader := rf.Propose(groupOp{Type: "Config", Config: cfg3}); !isLeader {
		t.Fatal("Propose(Config) should succeed on the leader")
	}
	// A Noop entry proposed right after cfg3 is a sync barrier: once IT has
	// applied, cfg3 (proposed earlier, so logged at an earlier index) is
	// guaranteed to have applied too.
	term := rf.Term()
	if _, _, isLeader := rf.Propose(groupOp{Type: "Noop"}); !isLeader {
		t.Fatal("Propose(Noop) should succeed on the leader")
	}
	waitFor(t, time.Second, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.noopAppliedTerm == term
	})

	s.mu.Lock()
	got := s.cfg.Num
	s.mu.Unlock()
	if got != 1 {
		t.Fatalf("a Config that skips a version (1 -> 3, missing 2) must be rejected: s.cfg.Num = %d, want 1", got)
	}
}
