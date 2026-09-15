package kvstore

import (
	"testing"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
)

// otherPeers is a small local copy of the same helper 01-raft's own test
// files use — it's unexported and lives in a _test.go file there, so it
// isn't importable across packages; duplicating three lines is simpler
// and more honest than exporting a test-only helper from a production
// package just to share it.
func otherPeers(ids []int, self int) []int {
	peers := make([]int, 0, len(ids)-1)
	for _, id := range ids {
		if id != self {
			peers = append(peers, id)
		}
	}
	return peers
}

// waitFor polls condition until it's true or timeout elapses, failing the
// test if it never becomes true — same reasoning as 01-raft's own
// waitFor: a fixed sleep either wastes time or is flaky, polling succeeds
// the instant the real condition is true.
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

// TestApplyLoopAppliesPutAndAppend proves Day 1's headline requirement:
// Put sets a key, Append concatenates onto whatever's already there.
func TestApplyLoopAppliesPutAndAppend(t *testing.T) {
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

	if _, _, ok := rf.Propose(Op{Type: "Put", Key: "x", Value: "1", ClientID: 1, SeqNum: 1}); !ok {
		t.Fatal("Propose(Put) should succeed on the leader")
	}
	if _, _, ok := rf.Propose(Op{Type: "Append", Key: "x", Value: "-more", ClientID: 1, SeqNum: 2}); !ok {
		t.Fatal("Propose(Append) should succeed on the leader")
	}

	waitFor(t, time.Second, func() bool {
		v, ok := kv.get("x")
		return ok && v == "1-more"
	})
}

// TestApplyLoopPreservesCommitOrder proves the apply loop applies entries
// in the order Raft committed them, not just "eventually applies
// everything" — four Appends in a specific order must produce exactly
// that concatenation, not some other ordering that happens to use the
// same characters.
func TestApplyLoopPreservesCommitOrder(t *testing.T) {
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

	for i, part := range []string{"a", "b", "c", "d"} {
		op := Op{Type: "Append", Key: "seq", Value: part, ClientID: 1, SeqNum: int64(i + 1)}
		if _, _, ok := rf.Propose(op); !ok {
			t.Fatalf("Propose(Append %q) should succeed", part)
		}
	}

	waitFor(t, time.Second, func() bool {
		v, ok := kv.get("seq")
		return ok && v == "abcd"
	})
}

// TestGetReturnsFalseForMissingKey proves the not-found case is
// distinguishable from a key that's genuinely set to "".
func TestGetReturnsFalseForMissingKey(t *testing.T) {
	rf := raft.NewRaft(0, nil, raft.NewFakeTransport())
	go rf.RunApplyLoop()
	defer rf.StopElectionTimer()

	kv := NewKVServer(rf)
	if v, ok := kv.get("nope"); ok || v != "" {
		t.Fatalf("Get(missing key) = (%q, %v), want (\"\", false)", v, ok)
	}
}

// TestKVStoreConvergesAcrossCluster is Day 1's real payoff, end-to-end:
// a 3-node Raft cluster, each running its OWN KVServer with its OWN
// independent apply loop, converges to the identical final state after
// the leader's Proposed commands replicate and commit. This is the
// entire point of a replicated state machine — proving it holds is more
// than any single-node test can show.
func TestKVStoreConvergesAcrossCluster(t *testing.T) {
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

	if _, _, ok := nodes[leaderID].Propose(Op{Type: "Put", Key: "x", Value: "1", ClientID: 1, SeqNum: 1}); !ok {
		t.Fatal("Propose(Put) should succeed on the leader")
	}
	if _, _, ok := nodes[leaderID].Propose(Op{Type: "Append", Key: "x", Value: "-more", ClientID: 1, SeqNum: 2}); !ok {
		t.Fatal("Propose(Append) should succeed on the leader")
	}

	for _, id := range ids {
		id := id
		waitFor(t, 20*raft.ElectionTimeoutMax, func() bool {
			v, ok := kvs[id].get("x")
			return ok && v == "1-more"
		})
	}
}

// TestOpGobRegistrationRoundTrips proves the exact failure mode Stage 1's
// persist.go warned about: a Command type that was never gob.Register'd
// fails at DECODE time, not compile time. Op is registered in this
// package's own init() (op.go) — if that registration were missing or
// wrong, reconstructing a Raft node from a persister holding an
// Op-containing log would panic inside restoreLocked.
func TestOpGobRegistrationRoundTrips(t *testing.T) {
	persister := raft.NewMemoryPersister()
	rf := raft.NewRaftWithPersister(0, nil, raft.NewFakeTransport(), persister)
	if err := rf.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := rf.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	if _, _, ok := rf.Propose(Op{Type: "Put", Key: "x", Value: "1", ClientID: 1, SeqNum: 1}); !ok {
		t.Fatal("Propose should succeed")
	}

	// Reconstructing against the SAME persister forces a real gob-decode
	// of the persisted, Op-containing log — this is where a missing
	// registration would surface.
	restarted := raft.NewRaftWithPersister(0, nil, raft.NewFakeTransport(), persister)
	if got, want := restarted.Term(), rf.Term(); got != want {
		t.Fatalf("restarted term = %d, want %d (the Op-containing log must have decoded successfully to get here at all)", got, want)
	}
}
