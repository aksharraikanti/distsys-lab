package shardkv

import (
	"fmt"
	"strings"
	"sync"
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
func newTestGroupCluster(n, gid int, ctrlers []*Ctrler, maxRaftState int) (nodes map[int]*raft.Raft, servers []*GroupServer, transport *raft.FakeTransport, cleanup func()) {
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
		servers[id] = NewGroupServer(rf, gid, NewCtrlerClerk(ctrlers), maxRaftState)
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

// wirePeers gives every replica of every group in groups a full directory
// of every group (itself included, harmlessly) — see GroupServer.peers'
// own doc comment for why this in-process resolution exists at all. Tests
// that never cause a real ownership TRANSFER (only a group's very first
// assignment, whose previous owner is gid 0) don't need this: applyLoop
// only ever looks a shard's source group up in peers once it's actually
// migrating something.
func wirePeers(groups map[int][]*GroupServer) {
	for _, servers := range groups {
		for _, s := range servers {
			s.SetPeers(groups)
		}
	}
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

	_, g1, _, g1Cleanup := newTestGroupCluster(3, 1, ctrlers, -1)
	defer g1Cleanup()
	_, g2, _, g2Cleanup := newTestGroupCluster(3, 2, ctrlers, -1)
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

	nodes, g1, _, g1Cleanup := newTestGroupCluster(3, 1, ctrlers, -1)
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

// TestGroupServerMoveMigratesDataAndShardClerkFollows proves the end-to-end
// loop TASKS.md describes: once the controller reassigns a shard, the new
// owner PULLS the shard's real data (not just an empty map) from the old
// owner before it starts serving, the old owner starts refusing it once its
// own poll loop catches up, and ShardClerk — given no help beyond "the
// wrong group said no" — transparently refetches the config and finds the
// new owner, with the pre-Move value intact.
func TestGroupServerMoveMigratesDataAndShardClerkFollows(t *testing.T) {
	_, ctrlers, _, ctrlerCleanup := newTestCtrlerCluster(3)
	defer ctrlerCleanup()
	admin := NewCtrlerClerk(ctrlers)
	admin.Join(map[int][]string{1: addrsFor(1), 2: addrsFor(2)})
	cfg := admin.Query(-1)

	nodes1, g1, _, g1Cleanup := newTestGroupCluster(3, 1, ctrlers, -1)
	defer g1Cleanup()
	nodes2, g2, _, g2Cleanup := newTestGroupCluster(3, 2, ctrlers, -1)
	defer g2Cleanup()
	groups := map[int][]*GroupServer{1: g1, 2: g2}
	wirePeers(groups)

	ck := NewShardClerk(admin, groups)

	key := shardOwnedByGroup(cfg, 1)
	shard := Key2Shard(key)

	waitForGroupConfig(t, g1[waitForGroupLeader(t, nodes1, 2*time.Second)], cfg.Num, 2*time.Second)
	waitForGroupConfig(t, g2[waitForGroupLeader(t, nodes2, 2*time.Second)], cfg.Num, 2*time.Second)

	// Write BEFORE the Move — this is the whole point Day 4's version of
	// this test explicitly deferred: proving the value survives the hand-
	// off, not just that routing eventually points somewhere.
	ck.Put(key, "before-move")

	admin.Move(shard, 2)
	afterMove := admin.Query(-1)

	waitForGroupConfig(t, g1[waitForGroupLeader(t, nodes1, 2*time.Second)], afterMove.Num, 3*time.Second)
	waitForGroupConfig(t, g2[waitForGroupLeader(t, nodes2, 2*time.Second)], afterMove.Num, 3*time.Second)

	// ShardClerk still has the OLD config cached; it must transparently
	// refetch on GroupErrWrongGroup, find group 2, and see the MIGRATED
	// value — not "" (which a routing-only fix, with no real data pull,
	// would have returned). Bounded, not a bare call, so a real bug fails
	// the test instead of hanging it.
	done := make(chan string, 1)
	go func() { done <- ck.Get(key) }()
	select {
	case got := <-done:
		if got != "before-move" {
			t.Fatalf("Get(%q) after Move = %q, want the migrated value %q", key, got, "before-move")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ShardClerk never converged on the new owner after Move")
	}

	// A further write through the new owner must also work.
	ck.Put(key, "after-move")
	if got := ck.Get(key); got != "after-move" {
		t.Fatalf("Get(%q) after a post-migration write = %q, want %q", key, got, "after-move")
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

	nodes, g1, transport, g1Cleanup := newTestGroupCluster(3, 1, ctrlers, -1)
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

	// Partition, not Unregister: Unregister only blocks calls DIRECTED AT
	// the unregistered id, so the leader's own outgoing AppendEntries to
	// its (still registered) followers keep going through unaffected and
	// it just keeps committing normally — never forcing a genuine
	// leadership change, which is what this test's name claims to force.
	// Partition blocks both directions.
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
		t.Fatal("Put never completed after a leader cutoff")
	}
	transport.Heal()

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
		migrating:      make(map[int]int),
		leaving:        make(map[int]int),
		notifyChans:    make(map[int]chan groupApplyResult),
		duplicateTable: make(map[int64]int64),
		maxRaftState:   -1,
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
		migrating:      make(map[int]int),
		leaving:        make(map[int]int),
		notifyChans:    make(map[int]chan groupApplyResult),
		duplicateTable: make(map[int64]int64),
		maxRaftState:   -1,
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
	// to serve it): check s.store directly. Getting ownership back to gid 1
	// at this point would require a real migration FROM gid 2 (Day 5), which
	// this test has no donor wired up for — the store itself is the
	// authority here, not another round trip through Get.
	s.mu.Lock()
	_, stillSet := s.store[key]
	s.mu.Unlock()
	if stillSet {
		t.Fatalf("the rejected Put must never have mutated the store: store[%q] is set", key)
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
		migrating:      make(map[int]int),
		leaving:        make(map[int]int),
		notifyChans:    make(map[int]chan groupApplyResult),
		duplicateTable: make(map[int64]int64),
		maxRaftState:   -1,
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

// TestGroupServerDedupTableMovesWithShard is TASKS.md's own named hazard:
// "the dedup table moves WITH the shard... otherwise a client retry that
// spans a migration double-applies — Stage 2 Day 3's bug, reintroduced."
// A client Appends once (accepted, SeqNum recorded on the OLD owner), the
// shard migrates for real (through the actual Pull/Migrate protocol, peers
// wired, no shortcuts), and the SAME client retries the IDENTICAL request
// directly against the NEW owner — simulating a client that never saw the
// first ack and resent it after the shard had already moved. The new owner
// must recognize it as already-applied, not append "x" a second time.
func TestGroupServerDedupTableMovesWithShard(t *testing.T) {
	_, ctrlers, _, ctrlerCleanup := newTestCtrlerCluster(3)
	defer ctrlerCleanup()
	admin := NewCtrlerClerk(ctrlers)
	admin.Join(map[int][]string{1: addrsFor(1), 2: addrsFor(2)})
	cfg := admin.Query(-1)

	nodes1, g1, _, g1Cleanup := newTestGroupCluster(3, 1, ctrlers, -1)
	defer g1Cleanup()
	nodes2, g2, _, g2Cleanup := newTestGroupCluster(3, 2, ctrlers, -1)
	defer g2Cleanup()
	groups := map[int][]*GroupServer{1: g1, 2: g2}
	wirePeers(groups)

	key := shardOwnedByGroup(cfg, 1)
	shard := Key2Shard(key)

	g1LeaderID := waitForGroupLeader(t, nodes1, 2*time.Second)
	waitForGroupConfig(t, g1[g1LeaderID], cfg.Num, 2*time.Second)

	args := &GroupPutAppendArgs{Key: key, Value: "x", Op: "Append", ClientID: 55, SeqNum: 1}
	var r1 GroupPutAppendReply
	if err := g1[g1LeaderID].PutAppend(args, &r1); err != nil || r1.Err != GroupOK {
		t.Fatalf("initial Append on the old owner failed: err=%v reply=%+v", err, r1)
	}

	admin.Move(shard, 2)
	afterMove := admin.Query(-1)
	waitForGroupConfig(t, g1[waitForGroupLeader(t, nodes1, 2*time.Second)], afterMove.Num, 3*time.Second)
	g2LeaderID := waitForGroupLeader(t, nodes2, 2*time.Second)
	waitForGroupConfig(t, g2[g2LeaderID], afterMove.Num, 3*time.Second)

	// Wait for the actual pull to land (not just the config transition) —
	// Get stops saying GroupErrWrongGroup once migration completes.
	waitFor(t, 3*time.Second, func() bool {
		var reply GroupGetReply
		err := g2[g2LeaderID].Get(&GroupGetArgs{Key: key}, &reply)
		return err == nil && reply.Err != GroupErrWrongGroup
	})

	// The identical retry, same ClientID+SeqNum, now against the NEW owner.
	var r2 GroupPutAppendReply
	if err := g2[g2LeaderID].PutAppend(args, &r2); err != nil || r2.Err != GroupOK {
		t.Fatalf("retried Append on the new owner failed: err=%v reply=%+v", err, r2)
	}

	var g GroupGetReply
	if err := g2[g2LeaderID].Get(&GroupGetArgs{Key: key}, &g); err != nil || g.Err != GroupOK {
		t.Fatalf("Get on the new owner failed: err=%v reply=%+v", err, g)
	}
	if g.Value != "x" {
		t.Fatalf("a retry that spans a migration must not double-apply: value = %q, want %q", g.Value, "x")
	}
}

// TestGroupServerRefusesShardUntilMigrationCompletes is a whitebox
// reproduction of the exact window TASKS.md calls out: "operations on a
// shard mid-move must wait or fail with ErrWrongGroup, never be lost or
// served stale." Config and Migrate entries are proposed by hand — no
// configPollLoop or migrationLoop running — to freeze "ownership granted,
// data not yet pulled" open long enough to observe it, the same technique
// Day 3's and Day 4's own whitebox no-op-gate tests use for their windows.
func TestGroupServerRefusesShardUntilMigrationCompletes(t *testing.T) {
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
		gid:            2,
		ctrl:           NewCtrlerClerk(ctrlers),
		store:          make(map[string]string),
		migrating:      make(map[int]int),
		leaving:        make(map[int]int),
		notifyChans:    make(map[int]chan groupApplyResult),
		duplicateTable: make(map[int64]int64),
		maxRaftState:   -1,
		stopCh:         make(chan struct{}),
	}
	go s.applyLoop()
	defer s.Stop()

	key := "probe-0"
	shard := Key2Shard(key)

	v1 := Config{Num: 1, Groups: map[int][]string{1: addrsFor(1), 2: addrsFor(2)}}
	v1.Shards[shard] = 1 // owned by gid 1, not this server (gid 2)
	if _, _, isLeader := rf.Propose(groupOp{Type: "Config", Config: v1}); !isLeader {
		t.Fatal("Propose(Config) should succeed on the leader")
	}
	waitForGroupConfig(t, s, 1, time.Second)

	v2 := v1
	v2.Num = 2
	v2.Shards[shard] = 2 // now gained by this server, from a REAL prior owner
	term := rf.Term()
	if _, _, isLeader := rf.Propose(groupOp{Type: "Config", Config: v2}); !isLeader {
		t.Fatal("Propose(Config) should succeed on the leader")
	}
	if _, _, isLeader := rf.Propose(groupOp{Type: "Noop"}); !isLeader {
		t.Fatal("Propose(Noop) should succeed on the leader")
	}
	waitFor(t, time.Second, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.cfg.Num == 2 && s.noopAppliedTerm == term
	})

	var before GroupGetReply
	if err := s.Get(&GroupGetArgs{Key: key}, &before); err != nil {
		t.Fatalf("Get RPC itself should not error: %v", err)
	}
	if before.Err != GroupErrWrongGroup {
		t.Fatalf("Get on a shard gained but not yet migrated must refuse, got %v", before.Err)
	}

	if _, _, isLeader := rf.Propose(groupOp{
		Type: "Migrate", Shard: shard,
		Data:     map[string]string{key: "pulled"},
		DupTable: map[int64]int64{7: 3},
	}); !isLeader {
		t.Fatal("Propose(Migrate) should succeed on the leader")
	}
	waitFor(t, time.Second, func() bool {
		var r GroupGetReply
		return s.Get(&GroupGetArgs{Key: key}, &r) == nil && r.Err == GroupOK
	})

	var after GroupGetReply
	if err := s.Get(&GroupGetArgs{Key: key}, &after); err != nil || after.Value != "pulled" {
		t.Fatalf("Get after migration lands = (err=%v, value=%q), want (nil, %q)", err, after.Value, "pulled")
	}
	s.mu.Lock()
	gotSeq := s.duplicateTable[7]
	s.mu.Unlock()
	if gotSeq != 3 {
		t.Fatalf("migrated dedup entry not merged: duplicateTable[7] = %d, want 3", gotSeq)
	}
}

// TestGroupServerPullGates checks Pull's two "not ready" conditions
// directly — neither needs a raft cluster at all, since Pull never touches
// s.rf: it answers purely from cfg/migrating/store under s.mu.
func TestGroupServerPullGates(t *testing.T) {
	t.Run("refuses until donor catches up to the transition", func(t *testing.T) {
		donor := &GroupServer{
			gid:            1,
			cfg:            Config{Num: 1},
			store:          map[string]string{"k": "v"},
			migrating:      make(map[int]int),
			duplicateTable: make(map[int64]int64),
		}
		var reply PullReply
		if err := donor.Pull(&PullArgs{Shard: 0, ConfigNum: 2}, &reply); err != nil {
			t.Fatalf("Pull RPC itself should not error: %v", err)
		}
		if reply.Err != GroupErrNotReady {
			t.Fatalf("Pull asking for a config version the donor hasn't reached yet must refuse, got %v", reply.Err)
		}

		donor.cfg.Num = 2
		var reply2 PullReply
		if err := donor.Pull(&PullArgs{Shard: 0, ConfigNum: 2}, &reply2); err != nil {
			t.Fatalf("Pull RPC itself should not error: %v", err)
		}
		if reply2.Err != GroupOK {
			t.Fatalf("Pull once the donor has caught up should succeed, got %v", reply2.Err)
		}
	})

	t.Run("refuses a shard the donor itself is still migrating in", func(t *testing.T) {
		donor := &GroupServer{
			gid:            2,
			cfg:            Config{Num: 5},
			store:          make(map[string]string),
			migrating:      map[int]int{3: 1}, // donor's own pull from gid 1 still pending
			duplicateTable: make(map[int64]int64),
		}
		var reply PullReply
		if err := donor.Pull(&PullArgs{Shard: 3, ConfigNum: 1}, &reply); err != nil {
			t.Fatalf("Pull RPC itself should not error: %v", err)
		}
		if reply.Err != GroupErrNotReady {
			t.Fatalf("Pull for a shard the donor is itself still migrating in must refuse, got %v", reply.Err)
		}
	})
}

// TestGroupServerMigratedDedupNeverRegresses checks the OTHER half of
// applyLoop's Migrate merge: take the MAX SeqNum per ClientID, never just
// overwrite. Every other test's migrated DupTable entries land on a client
// this recipient has never seen before, so a plain overwrite would have
// passed them too — this one specifically gives the recipient a HIGHER
// SeqNum for a client BEFORE the migration, from an independent write to a
// shard it already owned, then migrates in a LOWER, stale SeqNum for that
// same client. Overwriting would walk duplicateTable backwards and let an
// already-applied write be re-accepted as if it were new.
func TestGroupServerMigratedDedupNeverRegresses(t *testing.T) {
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
		gid:            2,
		ctrl:           NewCtrlerClerk(ctrlers),
		store:          make(map[string]string),
		migrating:      make(map[int]int),
		leaving:        make(map[int]int),
		notifyChans:    make(map[int]chan groupApplyResult),
		duplicateTable: make(map[int64]int64),
		maxRaftState:   -1,
		stopCh:         make(chan struct{}),
	}
	go s.applyLoop()
	defer s.Stop()

	// This server already owns every shard EXCEPT shard 0 from the start
	// (shard 0 is what migrates in later), and has already applied client
	// 42's SeqNum 5 write directly, to a key on one of the shards it
	// already owns.
	own := Config{Num: 1, Groups: map[int][]string{1: addrsFor(1), 2: addrsFor(2)}}
	for i := range own.Shards {
		own.Shards[i] = 2
	}
	own.Shards[0] = 1
	if _, _, isLeader := rf.Propose(groupOp{Type: "Config", Config: own}); !isLeader {
		t.Fatal("Propose(Config) should succeed on the leader")
	}
	waitForGroupConfig(t, s, 1, time.Second)
	ownedKey := shardOwnedByGroup(own, 2)
	writeErr := s.proposeAndWait(groupOp{Type: "Put", Key: ownedKey, Value: "v", ClientID: 42, SeqNum: 5})
	if writeErr != GroupOK {
		t.Fatalf("initial write failed: %v", writeErr)
	}
	s.mu.Lock()
	before := s.duplicateTable[42]
	s.mu.Unlock()
	if before != 5 {
		t.Fatalf("duplicateTable[42] = %d after the initial write, want 5", before)
	}

	// Now gain shard 0 from gid 1, and migrate in a table where client 42's
	// entry is STALE (SeqNum 3, lower than what this replica already has).
	gain := own
	gain.Num = 2
	gain.Shards[0] = 2
	if _, _, isLeader := rf.Propose(groupOp{Type: "Config", Config: gain}); !isLeader {
		t.Fatal("Propose(Config) should succeed on the leader")
	}
	waitForGroupConfig(t, s, 2, time.Second)
	if _, _, isLeader := rf.Propose(groupOp{
		Type: "Migrate", Shard: 0,
		Data:     map[string]string{},
		DupTable: map[int64]int64{42: 3},
	}); !isLeader {
		t.Fatal("Propose(Migrate) should succeed on the leader")
	}
	waitFor(t, time.Second, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		_, migrating := s.migrating[0]
		return !migrating
	})

	s.mu.Lock()
	after := s.duplicateTable[42]
	s.mu.Unlock()
	if after != 5 {
		t.Fatalf("a stale migrated dedup entry (SeqNum 3) must not regress duplicateTable[42]: got %d, want 5", after)
	}
}

// TestGroupServerRecipientReadyGate unit-tests recipientReady (and,
// through it, HasShard) directly: this is the SAFETY gate that stops gcLoop
// from deleting a shard before the new owner genuinely has it. The full
// end-to-end GC test below exercises the real protocol, but its timing
// (migration and GC racing concurrently) can't reliably prove this specific
// gate is what's doing the work — a broken gate that always says "ready"
// only causes an observable failure on an unlucky interleaving, not every
// run. This test makes the gate's own logic deterministic to check.
func TestGroupServerRecipientReadyGate(t *testing.T) {
	donor := &GroupServer{} // recipientReady only touches its arguments

	notYetOwner := &GroupServer{gid: 2, cfg: Config{Num: 1}, migrating: make(map[int]int)}
	if donor.recipientReady([]*GroupServer{notYetOwner}, 0) {
		t.Fatal("recipientReady must be false when the recipient doesn't own the shard yet")
	}

	stillMigrating := &GroupServer{gid: 2, migrating: map[int]int{0: 1}}
	stillMigrating.cfg.Shards[0] = 2
	if donor.recipientReady([]*GroupServer{stillMigrating}, 0) {
		t.Fatal("recipientReady must be false when the recipient owns the shard but is still migrating it in")
	}

	ready := &GroupServer{gid: 2, migrating: make(map[int]int)}
	ready.cfg.Shards[0] = 2
	if !donor.recipientReady([]*GroupServer{ready}, 0) {
		t.Fatal("recipientReady must be true once the recipient owns the shard and isn't migrating it")
	}
}

// TestGroupServerGarbageCollectsMigratedShard is Day 6's own named
// "challenge": once a migrated shard's new owner is confirmed ready, the
// OLD owner must actually drop its now-orphaned copy — real data flows
// through the real Pull/Migrate/HasShard/GC protocol end to end here, no
// shortcuts, checked directly against EVERY replica's own store (not just
// the leader's) since GC is a log entry every replica applies identically.
func TestGroupServerGarbageCollectsMigratedShard(t *testing.T) {
	_, ctrlers, _, ctrlerCleanup := newTestCtrlerCluster(3)
	defer ctrlerCleanup()
	admin := NewCtrlerClerk(ctrlers)
	admin.Join(map[int][]string{1: addrsFor(1), 2: addrsFor(2)})
	cfg := admin.Query(-1)

	nodes1, g1, _, g1Cleanup := newTestGroupCluster(3, 1, ctrlers, -1)
	defer g1Cleanup()
	nodes2, g2, _, g2Cleanup := newTestGroupCluster(3, 2, ctrlers, -1)
	defer g2Cleanup()
	groups := map[int][]*GroupServer{1: g1, 2: g2}
	wirePeers(groups)

	ck := NewShardClerk(admin, groups)
	key := shardOwnedByGroup(cfg, 1)
	shard := Key2Shard(key)

	waitForGroupConfig(t, g1[waitForGroupLeader(t, nodes1, 2*time.Second)], cfg.Num, 2*time.Second)
	ck.Put(key, "gc-me")

	admin.Move(shard, 2)
	afterMove := admin.Query(-1)
	waitForGroupConfig(t, g1[waitForGroupLeader(t, nodes1, 2*time.Second)], afterMove.Num, 3*time.Second)
	waitForGroupConfig(t, g2[waitForGroupLeader(t, nodes2, 2*time.Second)], afterMove.Num, 3*time.Second)

	// Confirm the new owner really has it first — makes the test's intent
	// legible (the old owner isn't dropping the only copy) even though
	// gcLoop verifies this itself via HasShard regardless.
	waitFor(t, 3*time.Second, func() bool {
		var reply GroupGetReply
		err := g2[waitForGroupLeader(t, nodes2, 2*time.Second)].Get(&GroupGetArgs{Key: key}, &reply)
		return err == nil && reply.Err == GroupOK && reply.Value == "gc-me"
	})

	// Every replica of the OLD owner must eventually drop the key AND clear
	// its leaving bookkeeping — not just its current leader.
	waitFor(t, 3*time.Second, func() bool {
		for _, s := range g1 {
			s.mu.Lock()
			_, stillSet := s.store[key]
			stillLeaving := len(s.leaving)
			s.mu.Unlock()
			if stillSet || stillLeaving != 0 {
				return false
			}
		}
		return true
	})
}

// TestConcurrentClientsThroughReconfiguration is the invariant TASKS.md
// names for this day: "every acknowledged write is visible, none lost, none
// applied twice" — Stage 2 Day 5's own private-key-per-client trick, now
// run while the cluster grows from 1 group to 3, shards get explicitly
// Moved, and the original group Leaves entirely, all mid-flight. Each
// simulated client Appends a unique marker per call to a key only IT ever
// writes; if the final value isn't exactly the concatenation of every
// marker this client saw acknowledged, in order, something was lost,
// duplicated, or reordered somewhere underneath — which migration, GC, and
// reconfiguration all touch at once here.
func TestConcurrentClientsThroughReconfiguration(t *testing.T) {
	_, ctrlers, _, ctrlerCleanup := newTestCtrlerCluster(3)
	defer ctrlerCleanup()
	admin := NewCtrlerClerk(ctrlers)

	nodes1, g1, _, g1Cleanup := newTestGroupCluster(3, 1, ctrlers, -1)
	defer g1Cleanup()
	_, g2, _, g2Cleanup := newTestGroupCluster(3, 2, ctrlers, -1)
	defer g2Cleanup()
	_, g3, _, g3Cleanup := newTestGroupCluster(3, 3, ctrlers, -1)
	defer g3Cleanup()
	groups := map[int][]*GroupServer{1: g1, 2: g2, 3: g3}
	wirePeers(groups) // groups 2 and 3 aren't in any config yet — harmless

	admin.Join(map[int][]string{1: addrsFor(1)})
	waitForGroupConfig(t, g1[waitForGroupLeader(t, nodes1, 2*time.Second)], 1, 2*time.Second)

	const numClients = 5
	const opsPerClient = 30

	acked := make([][]string, numClients)
	var wg sync.WaitGroup
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Each simulated client gets its OWN CtrlerClerk, not the
			// shared admin one — CtrlerClerk (like ShardClerk) is
			// documented as unsafe for concurrent use, and admin is
			// already being driven concurrently by the reconfiguration
			// goroutine below.
			ck := NewShardClerk(NewCtrlerClerk(ctrlers), groups)
			key := fmt.Sprintf("client-%d", i)
			var own []string
			for j := 0; j < opsPerClient; j++ {
				marker := fmt.Sprintf("[%d-%d]", i, j)
				ck.Append(key, marker)
				own = append(own, marker)
			}
			acked[i] = own
		}(i)
	}

	// Reconfigure WHILE clients are hammering keys: grow from 1 group to 3,
	// explicitly Move a couple of shards, then remove the ORIGINAL group
	// entirely, forcing real migrations to fire during live traffic rather
	// than waiting for a quiet moment.
	go func() {
		time.Sleep(3 * time.Millisecond)
		admin.Join(map[int][]string{2: addrsFor(2), 3: addrsFor(3)})
		time.Sleep(5 * time.Millisecond)
		admin.Move(0, 2)
		admin.Move(NShards-1, 3)
		time.Sleep(5 * time.Millisecond)
		admin.Leave([]int{1})
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("clients never finished through reconfiguration")
	}

	verifyCk := NewShardClerk(NewCtrlerClerk(ctrlers), groups)
	for i := 0; i < numClients; i++ {
		key := fmt.Sprintf("client-%d", i)
		want := strings.Join(acked[i], "")
		got := verifyCk.Get(key)
		if got != want {
			t.Fatalf("client %d: final value has length %d, want %d (every acknowledged write must be visible exactly once, in order)", i, len(got), len(want))
		}
	}
}
