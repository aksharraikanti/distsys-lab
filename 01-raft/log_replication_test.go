package raft

import (
	"reflect"
	"sync"
	"testing"
)

// recordingTransport wraps a FakeTransport and records every
// AppendEntriesArgs sent to each peer, so tests can inspect exactly what
// a replication round carried — not just its end effect on a follower's
// log.
type recordingTransport struct {
	*FakeTransport
	mu    sync.Mutex
	calls map[int][]*AppendEntriesArgs
}

func newRecordingTransport() *recordingTransport {
	return &recordingTransport{
		FakeTransport: NewFakeTransport(),
		calls:         make(map[int][]*AppendEntriesArgs),
	}
}

func (t *recordingTransport) CallAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	t.mu.Lock()
	t.calls[peer] = append(t.calls[peer], args)
	t.mu.Unlock()
	return t.FakeTransport.CallAppendEntries(peer, args, reply)
}

func (t *recordingTransport) callsFor(peer int) []*AppendEntriesArgs {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]*AppendEntriesArgs(nil), t.calls[peer]...)
}

// alwaysRejectHandler is a minimal RPCHandler that always refuses
// AppendEntries at a fixed term — used to exercise replicateToPeer's
// nextIndex backoff-on-failure path directly. Nothing in the live system
// can produce a real AppendEntries rejection yet (that's Day 10's log
// consistency check), so this is how the "retries on failure" half of
// Day 8's requirement gets proven ahead of a real caller existing.
type alwaysRejectHandler struct {
	term int
}

func (h *alwaysRejectHandler) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	reply.Term = h.term
	reply.VoteGranted = false
	return nil
}

func (h *alwaysRejectHandler) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	reply.Term = h.term
	reply.Success = false
	return nil
}

// TestBecomeLeaderInitializesReplicationState proves the Raft paper's
// Figure 2 "reinitialized after election" rule: nextIndex starts
// optimistically at (leader's last log index + 1), matchIndex starts at
// 0 (nothing confirmed yet), for every peer.
func TestBecomeLeaderInitializesReplicationState(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	if err := r.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := r.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for _, peer := range []int{1, 2} {
		if got := r.nextIndex[peer]; got != 1 {
			t.Fatalf("nextIndex[%d] = %d, want 1 (empty log: last index 0, + 1)", peer, got)
		}
		if got := r.matchIndex[peer]; got != 0 {
			t.Fatalf("matchIndex[%d] = %d, want 0", peer, got)
		}
	}
}

// TestReplicateAdvancesMatchAndNextIndex proves a successful replication
// round updates both maps correctly for a follower that had nothing.
func TestReplicateAdvancesMatchAndNextIndex(t *testing.T) {
	transport := NewFakeTransport()
	ids := []int{0, 1, 2}
	nodes := make(map[int]*Raft, len(ids))
	for _, id := range ids {
		n := NewRaft(id, otherPeers(ids, id), transport)
		nodes[id] = n
		transport.Register(id, n)
	}

	if err := nodes[0].BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	nodes[0].startElection()
	if got := nodes[0].State(); got != Leader {
		t.Fatalf("node0 state = %s, want Leader", got)
	}

	if _, _, ok := nodes[0].Propose("x"); !ok {
		t.Fatal("Propose should succeed on the leader")
	}
	nodes[0].replicate()

	nodes[0].mu.Lock()
	defer nodes[0].mu.Unlock()
	for _, peer := range []int{1, 2} {
		if got := nodes[0].matchIndex[peer]; got != 1 {
			t.Fatalf("matchIndex[%d] = %d, want 1", peer, got)
		}
		if got := nodes[0].nextIndex[peer]; got != 2 {
			t.Fatalf("nextIndex[%d] = %d, want 2", peer, got)
		}
	}
}

// TestReplicateOnlySendsNewEntriesOnSubsequentRounds is Day 8's headline
// proof: after a follower is caught up, the next replication round only
// carries what's actually new (per nextIndex), not the whole log again —
// and the PrevLogIndex/PrevLogTerm sent along with it correctly describe
// where in the follower's log the new entry is meant to attach.
func TestReplicateOnlySendsNewEntriesOnSubsequentRounds(t *testing.T) {
	transport := newRecordingTransport()
	ids := []int{0, 1, 2}
	nodes := make(map[int]*Raft, len(ids))
	for _, id := range ids {
		n := NewRaft(id, otherPeers(ids, id), transport)
		nodes[id] = n
		transport.Register(id, n)
	}

	if err := nodes[0].BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	nodes[0].startElection()
	if got := nodes[0].State(); got != Leader {
		t.Fatalf("node0 state = %s, want Leader", got)
	}
	term := nodes[0].Term()

	if _, _, ok := nodes[0].Propose("cmd-1"); !ok {
		t.Fatal("Propose(cmd-1) should succeed")
	}
	nodes[0].replicate()

	if _, _, ok := nodes[0].Propose("cmd-2"); !ok {
		t.Fatal("Propose(cmd-2) should succeed")
	}
	nodes[0].replicate()

	nodes[1].mu.Lock()
	log := append([]LogEntry(nil), nodes[1].log...)
	nodes[1].mu.Unlock()
	want := []LogEntry{{Term: term, Command: "cmd-1"}, {Term: term, Command: "cmd-2"}}
	if !reflect.DeepEqual(log, want) {
		t.Fatalf("follower 1 log = %+v, want %+v", log, want)
	}

	calls := transport.callsFor(1)
	if len(calls) < 2 {
		t.Fatalf("expected at least 2 AppendEntries calls to peer 1, got %d", len(calls))
	}
	last := calls[len(calls)-1]
	if len(last.Entries) != 1 || last.Entries[0].Command != "cmd-2" {
		t.Fatalf("second replicate() round to peer 1 sent %+v, want exactly [cmd-2] (nextIndex should skip what's already confirmed)", last.Entries)
	}
	if last.PrevLogIndex != 1 || last.PrevLogTerm != term {
		t.Fatalf("second round PrevLogIndex/PrevLogTerm = %d/%d, want 1/%d (right after cmd-1)", last.PrevLogIndex, last.PrevLogTerm, term)
	}
}

// TestReplicateToPeerBacksOffNextIndexOnFailure proves the "retries on
// failure" half of Day 8's requirement directly, since nothing in the
// live system can produce a real AppendEntries rejection until Day 10's
// consistency check exists: each rejected round should back nextIndex off
// by exactly one.
func TestReplicateToPeerBacksOffNextIndexOnFailure(t *testing.T) {
	transport := NewFakeTransport()
	leader := NewRaft(0, []int{1}, transport)
	transport.Register(0, leader)
	transport.Register(1, &alwaysRejectHandler{term: 1}) // same term as the leader — a rejection, not a step-down

	if err := leader.BecomeCandidate(); err != nil { // term becomes 1
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := leader.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	for _, cmd := range []string{"a", "b", "c"} {
		if _, _, ok := leader.Propose(cmd); !ok {
			t.Fatalf("Propose(%q) should succeed", cmd)
		}
	}

	leader.mu.Lock()
	leader.nextIndex[1] = 4 // pretend the leader believed peer 1 was fully caught up
	leader.mu.Unlock()

	leader.replicateToPeer(1)
	leader.mu.Lock()
	got := leader.nextIndex[1]
	leader.mu.Unlock()
	if got != 3 {
		t.Fatalf("nextIndex[1] after one rejected round = %d, want 3 (backed off by 1)", got)
	}

	leader.replicateToPeer(1)
	leader.mu.Lock()
	got = leader.nextIndex[1]
	leader.mu.Unlock()
	if got != 2 {
		t.Fatalf("nextIndex[1] after two rejected rounds = %d, want 2", got)
	}
}
