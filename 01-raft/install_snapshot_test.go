package raft

import (
	"reflect"
	"testing"
	"time"
)

// TestInstallSnapshotRejectsStaleTerm proves a node ignores an
// InstallSnapshot from a leader whose term has already fallen behind —
// same rule AppendEntries and RequestVote already follow: a stale
// leader's RPCs never get to mutate anything here, only learn the
// current term so it can step down itself.
func TestInstallSnapshotRejectsStaleTerm(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	r.mu.Lock()
	r.currentTerm = 5
	r.log = []LogEntry{{Term: 1, Command: "a"}}
	r.mu.Unlock()

	args := &InstallSnapshotArgs{Term: 3, LeaderID: 1, LastIncludedIndex: 10, LastIncludedTerm: 3, Data: []byte("snap")}
	var reply InstallSnapshotReply
	if err := r.InstallSnapshot(args, &reply); err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}
	if reply.Term != 5 {
		t.Fatalf("reply.Term = %d, want 5 (this node's own, higher term)", reply.Term)
	}

	r.mu.Lock()
	lastIncluded := r.lastIncludedIndex
	logLen := len(r.log)
	r.mu.Unlock()
	if lastIncluded != 0 {
		t.Fatalf("lastIncludedIndex = %d, want 0 (a stale InstallSnapshot must not compact anything)", lastIncluded)
	}
	if logLen != 1 {
		t.Fatalf("log length = %d, want 1 (unchanged)", logLen)
	}
}

// TestInstallSnapshotDiscardsDivergedSuffix proves the paper's Figure 13
// "no match" branch: when this node's own log doesn't have an entry at
// LastIncludedIndex agreeing with LastIncludedTerm (either it's shorter,
// or it disagrees), nothing in that log is salvageable — it's discarded
// entirely, not just truncated to LastIncludedIndex, since even entries
// that happen to still be physically present past that point can't be
// trusted once the anchor point itself doesn't match.
func TestInstallSnapshotDiscardsDivergedSuffix(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	r.mu.Lock()
	r.currentTerm = 2
	// This node's own index 5 is term 1 — the snapshot claims term 2
	// there, a genuine divergence (e.g. this node's log came from an
	// old, superseded leader).
	r.log = []LogEntry{{Term: 1}, {Term: 1}, {Term: 1}, {Term: 1}, {Term: 1}, {Term: 1}}
	r.mu.Unlock()

	args := &InstallSnapshotArgs{Term: 2, LeaderID: 1, LastIncludedIndex: 5, LastIncludedTerm: 2, Data: []byte("snap")}
	var reply InstallSnapshotReply
	if err := r.InstallSnapshot(args, &reply); err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}

	r.mu.Lock()
	logLen := len(r.log)
	lastIncluded := r.lastIncludedIndex
	lastIncludedTerm := r.lastIncludedTerm
	r.mu.Unlock()
	if lastIncluded != 5 || lastIncludedTerm != 2 {
		t.Fatalf("lastIncludedIndex/Term = %d/%d, want 5/2", lastIncluded, lastIncludedTerm)
	}
	if logLen != 0 {
		t.Fatalf("log length after a diverged install = %d, want 0 (fully discarded)", logLen)
	}
}

// TestInstallSnapshotPreservesSuffixWhenTermsAgree proves the OTHER
// Figure 13 branch: when this node's log already has a real entry at
// LastIncludedIndex agreeing on term, whatever comes AFTER it is still
// legitimate and must be kept, not thrown away along with the prefix
// the snapshot subsumes.
func TestInstallSnapshotPreservesSuffixWhenTermsAgree(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	r.mu.Lock()
	r.currentTerm = 2
	r.log = []LogEntry{
		{Term: 1, Command: "a"}, // index 1
		{Term: 1, Command: "b"}, // index 2
		{Term: 2, Command: "c"}, // index 3 — matches the snapshot's claim
		{Term: 2, Command: "d"}, // index 4 — must survive
		{Term: 2, Command: "e"}, // index 5 — must survive
	}
	r.mu.Unlock()

	args := &InstallSnapshotArgs{Term: 2, LeaderID: 1, LastIncludedIndex: 3, LastIncludedTerm: 2, Data: []byte("snap")}
	var reply InstallSnapshotReply
	if err := r.InstallSnapshot(args, &reply); err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}

	r.mu.Lock()
	log := append([]LogEntry(nil), r.log...)
	r.mu.Unlock()
	want := []LogEntry{{Term: 2, Command: "d"}, {Term: 2, Command: "e"}}
	if !reflect.DeepEqual(log, want) {
		t.Fatalf("log after install = %+v, want %+v (entries 4-5 preserved)", log, want)
	}

	// And the preserved suffix must still be reachable at its correct
	// ABSOLUTE index, not reindexed from 0.
	r.mu.Lock()
	term, ok := r.termAtLocked(4)
	r.mu.Unlock()
	if !ok || term != 2 {
		t.Fatalf("termAtLocked(4) = (%d, %v), want (2, true)", term, ok)
	}
}

// TestInstallSnapshotIsNoOpForStaleOrDuplicateCall proves a call whose
// LastIncludedIndex is at or below what's already installed changes
// nothing — the situation a retried RPC, or a race between two
// InstallSnapshot calls, produces.
func TestInstallSnapshotIsNoOpForStaleOrDuplicateCall(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	r.mu.Lock()
	r.currentTerm = 2
	r.lastIncludedIndex = 10
	r.lastIncludedTerm = 2
	r.log = []LogEntry{{Term: 2, Command: "kept"}}
	r.mu.Unlock()

	args := &InstallSnapshotArgs{Term: 2, LeaderID: 1, LastIncludedIndex: 8, LastIncludedTerm: 2, Data: []byte("stale")}
	var reply InstallSnapshotReply
	if err := r.InstallSnapshot(args, &reply); err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}

	r.mu.Lock()
	lastIncluded := r.lastIncludedIndex
	log := append([]LogEntry(nil), r.log...)
	pending := r.pendingSnapshot
	r.mu.Unlock()
	if lastIncluded != 10 {
		t.Fatalf("lastIncludedIndex = %d, want 10 (unchanged by a stale call)", lastIncluded)
	}
	if !reflect.DeepEqual(log, []LogEntry{{Term: 2, Command: "kept"}}) {
		t.Fatalf("log changed after a stale InstallSnapshot: %+v", log)
	}
	if pending != nil {
		t.Fatalf("pendingSnapshot = %+v, want nil (a stale install has nothing new for the state machine)", pending)
	}
}

// TestInstallSnapshotDeliversToApplyCh proves the full pipeline from
// RPC to state machine: a fresh InstallSnapshot must arrive on ApplyCh
// as a SnapshotValid message carrying the right bytes/index/term — and,
// per pendingSnapshot's doc comment, delivered by RunApplyLoop's own
// goroutine, not sent directly from inside InstallSnapshot itself.
func TestInstallSnapshotDeliversToApplyCh(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	go r.RunApplyLoop()
	defer r.StopElectionTimer()

	args := &InstallSnapshotArgs{Term: 1, LeaderID: 1, LastIncludedIndex: 7, LastIncludedTerm: 1, Data: []byte("kv-state")}
	var reply InstallSnapshotReply
	if err := r.InstallSnapshot(args, &reply); err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}

	select {
	case msg := <-r.ApplyCh:
		if !msg.SnapshotValid {
			t.Fatalf("applied %+v, want SnapshotValid = true", msg)
		}
		if msg.SnapshotIndex != 7 || msg.SnapshotTerm != 1 || string(msg.Snapshot) != "kv-state" {
			t.Fatalf("applied %+v, want SnapshotIndex=7 SnapshotTerm=1 Snapshot=%q", msg, "kv-state")
		}
	case <-time.After(20 * ElectionTimeoutMax):
		t.Fatal("installed snapshot never arrived on ApplyCh")
	}
}

// TestInstallSnapshotBumpsCommitAndApplied proves commitIndex and
// lastApplied both advance to at least LastIncludedIndex on install —
// without this, applyPending would try to apply an index that no
// longer exists in the (now-trimmed) log, exactly the bug
// restoreLocked's equivalent bump (Day 6) exists to prevent on restart.
func TestInstallSnapshotBumpsCommitAndApplied(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	go r.RunApplyLoop()
	defer r.StopElectionTimer()

	args := &InstallSnapshotArgs{Term: 1, LeaderID: 1, LastIncludedIndex: 9, LastIncludedTerm: 1, Data: []byte("snap")}
	var reply InstallSnapshotReply
	if err := r.InstallSnapshot(args, &reply); err != nil {
		t.Fatalf("InstallSnapshot: %v", err)
	}

	if got := r.CommitIndex(); got < 9 {
		t.Fatalf("CommitIndex = %d, want >= 9", got)
	}
	r.mu.Lock()
	lastApplied := r.lastApplied
	r.mu.Unlock()
	if lastApplied < 9 {
		t.Fatalf("lastApplied = %d, want >= 9", lastApplied)
	}
}

// TestInstallSnapshotCatchesUpLaggingFollower is Day 7's headline proof,
// end to end: a follower gets disconnected, the leader keeps committing
// AND snapshots past what that follower ever saw, the follower
// reconnects — and must still converge, via InstallSnapshot, since
// AppendEntries alone can never succeed for it any more (the leader has
// nothing left to send it starting from where it left off).
func TestInstallSnapshotCatchesUpLaggingFollower(t *testing.T) {
	transport := NewFakeTransport()
	ids := []int{0, 1, 2}
	nodes := make(map[int]*Raft, len(ids))
	for _, id := range ids {
		n := NewRaft(id, otherPeers(ids, id), transport)
		nodes[id] = n
		transport.Register(id, n)
	}
	for _, n := range nodes {
		go n.RunElectionTimer()
		go n.RunHeartbeats()
		go n.RunApplyLoop()
	}
	defer func() {
		for _, n := range nodes {
			n.StopElectionTimer()
		}
	}()

	leaderID := waitForSingleLeader(t, nodes, 20*ElectionTimeoutMax)
	laggingID := -1
	for _, id := range ids {
		if id != leaderID {
			laggingID = id
			break
		}
	}

	// Disconnect the lagging follower entirely — it sees nothing from
	// here on until reconnected.
	transport.Unregister(laggingID)

	var lastIndex int
	for i := 0; i < 10; i++ {
		index, _, ok := nodes[leaderID].Propose("cmd")
		if !ok {
			t.Fatalf("Propose(%d): not leader", i)
		}
		lastIndex = index
	}
	// Only the reachable nodes (leader + the other follower) can reach
	// commitIndex=lastIndex — the disconnected one is excluded from
	// "all" on purpose here.
	reachable := map[int]*Raft{}
	for id, n := range nodes {
		if id != laggingID {
			reachable[id] = n
		}
	}
	waitForAllCommitAtLeast(t, reachable, lastIndex, 20*ElectionTimeoutMax)
	waitForLastAppliedAtLeast(t, nodes[leaderID], lastIndex, 20*ElectionTimeoutMax)

	// Compact the leader's log well past anything the lagging follower
	// ever received — its nextIndex, once reconnected, will point at an
	// index the leader can no longer serve via AppendEntries at all.
	if err := nodes[leaderID].Snapshot(lastIndex, []byte("caught-up-state")); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	transport.Register(laggingID, nodes[laggingID])

	// The lagging node must receive the installed snapshot on its own
	// ApplyCh — proving InstallSnapshot actually ran on it, not just
	// that its commitIndex happens to match some other way.
	select {
	case msg := <-nodes[laggingID].ApplyCh:
		if !msg.SnapshotValid || msg.SnapshotIndex < lastIndex {
			t.Fatalf("lagging node's first ApplyCh message = %+v, want a SnapshotValid message with SnapshotIndex >= %d", msg, lastIndex)
		}
	case <-time.After(40 * ElectionTimeoutMax):
		t.Fatal("lagging node never received an installed snapshot after reconnecting")
	}

	waitForAllCommitAtLeast(t, nodes, lastIndex, 40*ElectionTimeoutMax)

	// And the cluster must still be able to make forward progress
	// afterward — InstallSnapshot resuming ordinary AppendEntries
	// replication, not leaving the follower in some special state.
	newIndex, _, ok := nodes[leaderID].Propose("cmd-after-catchup")
	if !ok {
		t.Fatal("Propose after catch-up: not leader")
	}
	waitForAllCommitAtLeast(t, nodes, newIndex, 20*ElectionTimeoutMax)
}
