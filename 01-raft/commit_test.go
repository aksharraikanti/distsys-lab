package raft

import "testing"

// TestAdvanceCommitIndexRequiresCurrentTermEntry is the day's central
// correctness proof: the Raft paper's Figure 8 safety fix. A majority
// having replicated an entry from an OLDER term is NOT by itself enough
// to commit it — only once a later entry FROM THE LEADER'S CURRENT TERM
// also reaches a majority does everything before it become safe.
func TestAdvanceCommitIndexRequiresCurrentTermEntry(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())

	r.mu.Lock()
	r.state = Leader
	r.currentTerm = 2
	r.log = []LogEntry{{Term: 1, Command: "old"}} // index 1, from an OLDER term
	r.nextIndex = map[int]int{1: 2, 2: 2}
	r.matchIndex = map[int]int{1: 1, 2: 1} // both peers already confirmed index 1
	r.advanceCommitIndexLocked()
	gotAfterOldTermMajority := r.commitIndex
	r.mu.Unlock()

	if gotAfterOldTermMajority != 0 {
		t.Fatalf("commitIndex after a majority-replicated OLDER-term entry = %d, want 0 (must not commit directly)", gotAfterOldTermMajority)
	}

	// A second entry lands from the CURRENT term and also reaches a
	// majority — this is what actually makes both entries safe, per the
	// paper's resolution to Figure 8.
	r.mu.Lock()
	r.log = append(r.log, LogEntry{Term: 2, Command: "new"})
	r.matchIndex[1] = 2
	r.matchIndex[2] = 2
	r.advanceCommitIndexLocked()
	got := r.commitIndex
	r.mu.Unlock()

	if got != 2 {
		t.Fatalf("commitIndex after a majority-replicated CURRENT-term entry = %d, want 2 (both entries now safe)", got)
	}
}

// TestCommitIndexAdvancesOnMajorityReplication proves the ordinary case
// end-to-end through replicate(), not just the direct
// advanceCommitIndexLocked unit test above: a 3-node cluster's leader
// commits an entry once a majority (itself + one peer, out of three)
// confirms it.
func TestCommitIndexAdvancesOnMajorityReplication(t *testing.T) {
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

	if _, _, ok := nodes[0].Propose("cmd"); !ok {
		t.Fatal("Propose should succeed")
	}
	if got := nodes[0].CommitIndex(); got != 0 {
		t.Fatalf("commitIndex before replication = %d, want 0 (not yet confirmed by any peer)", got)
	}

	nodes[0].replicate()

	if got := nodes[0].CommitIndex(); got != 1 {
		t.Fatalf("commitIndex after majority replication = %d, want 1", got)
	}
}

// TestSingleNodeClusterCommitsImmediately proves the edge case that
// motivates calling advanceCommitIndexLocked from Propose, not only from
// replicateToPeer: with zero peers, a leader's own log always trivially
// satisfies a majority of one, so an entry commits the instant it's
// proposed — there's no peer reply to wait for.
func TestSingleNodeClusterCommitsImmediately(t *testing.T) {
	r := NewRaft(0, nil, NewFakeTransport())
	if err := r.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := r.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}

	index, _, ok := r.Propose("solo")
	if !ok {
		t.Fatal("Propose should succeed")
	}
	if got := r.CommitIndex(); got != index {
		t.Fatalf("commitIndex after Propose on a single-node cluster = %d, want %d (leader is its own majority)", got, index)
	}
}

// TestFollowerCommitIndexFollowsLeaderCommit proves the follower half of
// Day 9: commitIndex adopts LeaderCommit, capped at this node's own last
// log index — a leader can never claim more is committed than this node
// actually has appended.
func TestFollowerCommitIndexFollowsLeaderCommit(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())

	entries := []LogEntry{{Term: 1, Command: "a"}, {Term: 1, Command: "b"}, {Term: 1, Command: "c"}}
	args := &AppendEntriesArgs{Term: 1, LeaderID: 1, PrevLogIndex: 0, Entries: entries, LeaderCommit: 2}
	var reply AppendEntriesReply
	if err := r.AppendEntries(args, &reply); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if got := r.CommitIndex(); got != 2 {
		t.Fatalf("follower commitIndex = %d, want 2 (min(leaderCommit=2, lastNewIndex=3))", got)
	}

	// LeaderCommit beyond what this node actually has must be capped, not
	// taken at face value.
	args2 := &AppendEntriesArgs{Term: 1, LeaderID: 1, PrevLogIndex: 3, LeaderCommit: 10}
	var reply2 AppendEntriesReply
	if err := r.AppendEntries(args2, &reply2); err != nil {
		t.Fatalf("AppendEntries (heartbeat): %v", err)
	}
	if got := r.CommitIndex(); got != 3 {
		t.Fatalf("follower commitIndex after LeaderCommit(10) with only 3 entries = %d, want 3 (capped)", got)
	}
}
