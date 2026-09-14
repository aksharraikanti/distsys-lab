package kvstore

import (
	"testing"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
)

// TestPutAppendAndGetRoundTrip proves the headline requirement: a
// client's PutAppend commits, and a subsequent Get reflects it — the
// real RPC-shaped path, not the internal get() primitive Day 1's tests
// used directly.
func TestPutAppendAndGetRoundTrip(t *testing.T) {
	rf := raft.NewRaft(0, nil, raft.NewFakeTransport())
	if err := rf.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := rf.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	go rf.RunApplyLoop()
	defer rf.StopElectionTimer()

	kv := NewKVServer(rf)

	var putReply PutAppendReply
	if err := kv.PutAppend(&PutAppendArgs{Key: "x", Value: "1", Op: "Put"}, &putReply); err != nil {
		t.Fatalf("PutAppend: %v", err)
	}
	if putReply.Err != OK {
		t.Fatalf("PutAppend(Put) Err = %q, want OK", putReply.Err)
	}

	var appendReply PutAppendReply
	if err := kv.PutAppend(&PutAppendArgs{Key: "x", Value: "-more", Op: "Append"}, &appendReply); err != nil {
		t.Fatalf("PutAppend: %v", err)
	}
	if appendReply.Err != OK {
		t.Fatalf("PutAppend(Append) Err = %q, want OK", appendReply.Err)
	}

	var getReply GetReply
	if err := kv.Get(&GetArgs{Key: "x"}, &getReply); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if getReply.Err != OK || getReply.Value != "1-more" {
		t.Fatalf("Get(x) = (Err=%q, Value=%q), want (OK, \"1-more\")", getReply.Err, getReply.Value)
	}
}

// TestGetReturnsErrNoKeyForMissingKey proves the RPC-shaped Get
// distinguishes "key not set" from "key set to empty string" the same
// way the underlying get() primitive does.
func TestGetReturnsErrNoKeyForMissingKey(t *testing.T) {
	rf := raft.NewRaft(0, nil, raft.NewFakeTransport())
	if err := rf.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := rf.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	go rf.RunApplyLoop()
	defer rf.StopElectionTimer()

	kv := NewKVServer(rf)

	var reply GetReply
	if err := kv.Get(&GetArgs{Key: "nope"}, &reply); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if reply.Err != ErrNoKey {
		t.Fatalf("Get(missing key) Err = %q, want ErrNoKey", reply.Err)
	}
}

// TestGetRejectsWhenNotLeader and TestPutAppendRejectsWhenNotLeader prove
// both RPCs refuse to serve a request on a node that isn't (or doesn't
// believe itself to be) the leader — a fresh node defaults to Follower.
func TestGetRejectsWhenNotLeader(t *testing.T) {
	rf := raft.NewRaft(0, []int{1, 2}, raft.NewFakeTransport())
	go rf.RunApplyLoop()
	defer rf.StopElectionTimer()

	kv := NewKVServer(rf)
	var reply GetReply
	if err := kv.Get(&GetArgs{Key: "x"}, &reply); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if reply.Err != ErrWrongLeader {
		t.Fatalf("Get on a Follower Err = %q, want ErrWrongLeader", reply.Err)
	}
}

func TestPutAppendRejectsWhenNotLeader(t *testing.T) {
	rf := raft.NewRaft(0, []int{1, 2}, raft.NewFakeTransport())
	go rf.RunApplyLoop()
	defer rf.StopElectionTimer()

	kv := NewKVServer(rf)
	var reply PutAppendReply
	if err := kv.PutAppend(&PutAppendArgs{Key: "x", Value: "1", Op: "Put"}, &reply); err != nil {
		t.Fatalf("PutAppend: %v", err)
	}
	if reply.Err != ErrWrongLeader {
		t.Fatalf("PutAppend on a Follower Err = %q, want ErrWrongLeader", reply.Err)
	}
}

// TestPutAppendTimesOutWhenEntryNeverCommits proves PutAppend doesn't
// hang forever when its proposal can never reach a majority: an
// isolated leader (partitioned away from every peer) Proposes
// successfully but the entry can never commit — the handler must give up
// after commitTimeout rather than block the caller indefinitely.
func TestPutAppendTimesOutWhenEntryNeverCommits(t *testing.T) {
	transport := raft.NewFakeTransport()
	ids := []int{0, 1, 2}
	nodes := make(map[int]*raft.Raft, len(ids))
	kvs := make(map[int]*KVServer, len(ids))
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
	defer func() {
		for _, rf := range nodes {
			rf.StopElectionTimer()
		}
	}()

	leaderID := -1
	waitFor(t, 20*raft.ElectionTimeoutMax, func() bool {
		leaders := 0
		for id, rf := range nodes {
			if rf.State() == raft.Leader {
				leaders++
				leaderID = id
			}
		}
		return leaders == 1
	})

	// Isolate the leader completely — it can never reach a majority, so
	// nothing it Proposes can ever commit.
	transport.Partition([]int{leaderID}, otherPeers(ids, leaderID))
	defer transport.Heal()

	var reply PutAppendReply
	start := time.Now()
	if err := kvs[leaderID].PutAppend(&PutAppendArgs{Key: "x", Value: "1", Op: "Put"}, &reply); err != nil {
		t.Fatalf("PutAppend: %v", err)
	}
	elapsed := time.Since(start)

	if reply.Err != ErrTimeout {
		t.Fatalf("PutAppend on an isolated leader returned Err=%q, want ErrTimeout", reply.Err)
	}
	if elapsed < commitTimeout {
		t.Fatalf("PutAppend returned after %s, want it to have actually waited out the %s timeout", elapsed, commitTimeout)
	}
}

// TestPutAppendDetectsSupersededProposal is the scenario PutAppend's own
// doc comment names explicitly: this node Proposes an entry, loses
// leadership before it commits, and a NEW leader's different entry lands
// at that same log index instead. The waiting PutAppend call must detect
// the mismatch and report ErrWrongLeader — not falsely report success
// for an entry that got silently discarded, and not hang until the full
// commitTimeout when the real answer (superseded) is already knowable.
//
// This drives the scenario with real machinery, not hand-simulated
// internals: node0 becomes leader and Proposes while partitioned away
// from the rest of the cluster, so its entry can never commit; node1
// becomes leader for a higher term and commits its OWN entry at the
// same index; healing the partition delivers node1's entry to node0 via
// ordinary AppendEntries, which — per Day 10's consistency check —
// overwrites node0's still-uncommitted entry.
func TestPutAppendDetectsSupersededProposal(t *testing.T) {
	transport := raft.NewFakeTransport()
	ids := []int{0, 1, 2}
	nodes := make(map[int]*raft.Raft, len(ids))
	kvs := make(map[int]*KVServer, len(ids))
	for _, id := range ids {
		rf := raft.NewRaft(id, otherPeers(ids, id), transport)
		nodes[id] = rf
		transport.Register(id, rf)
		kvs[id] = NewKVServer(rf)
	}
	for _, rf := range nodes {
		go rf.RunHeartbeats()
		go rf.RunApplyLoop()
		// Deliberately NOT starting RunElectionTimer — leadership
		// transitions are driven manually below so the scenario is
		// fully deterministic instead of racing a real election clock.
	}
	defer func() {
		for _, rf := range nodes {
			rf.StopElectionTimer()
		}
	}()

	if err := nodes[0].BecomeCandidate(); err != nil { // term 1
		t.Fatalf("BecomeCandidate (node0): %v", err)
	}
	if err := nodes[0].BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader (node0): %v", err)
	}

	// Isolate node0 BEFORE it proposes anything — its own RunHeartbeats
	// loop must never get a chance to successfully replicate this entry.
	transport.Partition([]int{0}, []int{1, 2})

	replyCh := make(chan PutAppendReply, 1)
	go func() {
		var reply PutAppendReply
		_ = kvs[0].PutAppend(&PutAppendArgs{Key: "x", Value: "from-node0", Op: "Put"}, &reply)
		replyCh <- reply
	}()

	// Let the goroutine actually call Propose and register its notify
	// channel before node1 takes over.
	time.Sleep(10 * time.Millisecond)

	// Both nodes start at term 0, so a single BecomeCandidate call would
	// only tie node0's term, not exceed it. Two calls (Follower -> Candidate
	// -> Candidate, the legitimate "restart election" transition) reaches
	// term 2, deterministically higher than node0's term 1.
	if err := nodes[1].BecomeCandidate(); err != nil { // term 1
		t.Fatalf("BecomeCandidate (node1): %v", err)
	}
	if err := nodes[1].BecomeCandidate(); err != nil { // term 2 — now strictly higher than node0's
		t.Fatalf("BecomeCandidate (node1, retry): %v", err)
	}
	if err := nodes[1].BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader (node1): %v", err)
	}
	if _, _, ok := nodes[1].Propose(Op{Type: "Put", Key: "x", Value: "from-node1"}); !ok {
		t.Fatal("Propose on node1 should succeed")
	}

	// node1 + node2 is a majority (2 of 3) even with node0 still
	// isolated — this commits on its own via node1's running
	// RunHeartbeats loop.
	waitFor(t, 20*raft.ElectionTimeoutMax, func() bool {
		return nodes[1].CommitIndex() >= 1
	})

	transport.Heal()

	select {
	case reply := <-replyCh:
		if reply.Err != ErrWrongLeader {
			t.Fatalf("PutAppend on the superseded node0 returned Err=%q, want ErrWrongLeader", reply.Err)
		}
	case <-time.After(commitTimeout):
		t.Fatal("PutAppend never returned — it should have detected the superseded proposal well before the full commit timeout")
	}

	// node0's store must reflect node1's entry, not its own discarded one.
	waitFor(t, 20*raft.ElectionTimeoutMax, func() bool {
		v, ok := kvs[0].get("x")
		return ok && v == "from-node1"
	})
}
