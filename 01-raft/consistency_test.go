package raft

import (
	"reflect"
	"testing"
)

// TestAppendEntriesRejectsWhenPrevLogIndexBeyondLog proves a follower
// refuses an AppendEntries whose PrevLogIndex points past the end of its
// own log — it has nothing there to check the term against, so there's
// no way to confirm the leader's entries causally attach to anything
// real in this log.
func TestAppendEntriesRejectsWhenPrevLogIndexBeyondLog(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	r.mu.Lock()
	r.log = []LogEntry{{Term: 1, Command: "a"}}
	r.mu.Unlock()

	args := &AppendEntriesArgs{Term: 1, LeaderID: 1, PrevLogIndex: 5, PrevLogTerm: 1}
	var reply AppendEntriesReply
	if err := r.AppendEntries(args, &reply); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if reply.Success {
		t.Fatal("expected rejection — PrevLogIndex is beyond this node's log")
	}

	r.mu.Lock()
	log := append([]LogEntry(nil), r.log...)
	r.mu.Unlock()
	if !reflect.DeepEqual(log, []LogEntry{{Term: 1, Command: "a"}}) {
		t.Fatalf("log after a rejected AppendEntries = %+v, want unchanged", log)
	}
}

// TestAppendEntriesRejectsWhenPrevLogTermMismatch proves a follower
// refuses an AppendEntries whose PrevLogTerm doesn't match what's
// actually at that index — this is the core §5.3 consistency check, not
// just a bounds check.
func TestAppendEntriesRejectsWhenPrevLogTermMismatch(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	r.mu.Lock()
	r.log = []LogEntry{{Term: 1, Command: "a"}}
	r.mu.Unlock()

	args := &AppendEntriesArgs{Term: 1, LeaderID: 1, PrevLogIndex: 1, PrevLogTerm: 2} // this node's index 1 is term 1, not 2
	var reply AppendEntriesReply
	if err := r.AppendEntries(args, &reply); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if reply.Success {
		t.Fatal("expected rejection — PrevLogTerm doesn't match this node's log")
	}

	r.mu.Lock()
	log := append([]LogEntry(nil), r.log...)
	r.mu.Unlock()
	if !reflect.DeepEqual(log, []LogEntry{{Term: 1, Command: "a"}}) {
		t.Fatalf("log after a rejected AppendEntries = %+v, want unchanged", log)
	}
}

// TestAppendEntriesAcceptsWhenPrevLogMatches is the positive case: a
// correct PrevLogIndex/PrevLogTerm is accepted and the new entry lands.
func TestAppendEntriesAcceptsWhenPrevLogMatches(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	r.mu.Lock()
	r.log = []LogEntry{{Term: 1, Command: "a"}}
	r.mu.Unlock()

	args := &AppendEntriesArgs{
		Term: 1, LeaderID: 1, PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []LogEntry{{Term: 1, Command: "b"}},
	}
	var reply AppendEntriesReply
	if err := r.AppendEntries(args, &reply); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !reply.Success {
		t.Fatal("expected acceptance — PrevLogIndex/PrevLogTerm both match")
	}

	r.mu.Lock()
	log := append([]LogEntry(nil), r.log...)
	r.mu.Unlock()
	want := []LogEntry{{Term: 1, Command: "a"}, {Term: 1, Command: "b"}}
	if !reflect.DeepEqual(log, want) {
		t.Fatalf("follower log = %+v, want %+v", log, want)
	}
}

// TestAppendEntriesTruncatesConflictingSuffix proves §5.3's "delete the
// existing entry and all that follow it": a follower with a diverged
// suffix (entries from a term the leader never had) gets that suffix
// thrown away and replaced, once PrevLogIndex/PrevLogTerm confirm where
// the two logs actually still agree.
func TestAppendEntriesTruncatesConflictingSuffix(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	r.mu.Lock()
	r.log = []LogEntry{
		{Term: 1, Command: "a"},       // agrees with the leader
		{Term: 1, Command: "stale-b"}, // diverged: leader's real index-2 entry is different
		{Term: 1, Command: "stale-c"}, // must go too — follows the conflict
	}
	r.mu.Unlock()

	args := &AppendEntriesArgs{
		Term: 2, LeaderID: 1, PrevLogIndex: 1, PrevLogTerm: 1, // agrees up through index 1
		Entries: []LogEntry{{Term: 2, Command: "real-b"}},
	}
	var reply AppendEntriesReply
	if err := r.AppendEntries(args, &reply); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !reply.Success {
		t.Fatal("expected acceptance — PrevLogIndex/PrevLogTerm agree at index 1")
	}

	r.mu.Lock()
	log := append([]LogEntry(nil), r.log...)
	r.mu.Unlock()
	want := []LogEntry{{Term: 1, Command: "a"}, {Term: 2, Command: "real-b"}}
	if !reflect.DeepEqual(log, want) {
		t.Fatalf("follower log after truncation = %+v, want %+v (stale-b and stale-c both discarded)", log, want)
	}
}

// TestReplicateConvergesADivergedFollower is Day 10's headline proof: a
// follower whose log has genuinely diverged from the leader's (not just
// "is behind," but has a conflicting entry) gets corrected through the
// exact mechanism Day 8 built but couldn't yet exercise for real — a
// rejection backs nextIndex off by one, the next round tries one entry
// earlier, and this repeats until PrevLogIndex/PrevLogTerm finally agree.
func TestReplicateConvergesADivergedFollower(t *testing.T) {
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
	term := nodes[0].Term()

	for _, cmd := range []string{"a", "b", "c"} {
		if _, _, ok := nodes[0].Propose(cmd); !ok {
			t.Fatalf("Propose(%q) should succeed", cmd)
		}
	}

	// node1 diverged: its index-2 entry conflicts with what the leader
	// actually has there (term 99 vs. the leader's real term). The leader
	// believes node1 is already caught up through index 2 (nextIndex set
	// optimistically to 3), so the FIRST replication attempt will be
	// rejected — this is what forces the backoff-and-retry path to run
	// for real, not just converge in one trivial round.
	nodes[1].mu.Lock()
	nodes[1].currentTerm = term
	nodes[1].log = []LogEntry{
		{Term: term, Command: "a"},
		{Term: 99, Command: "wrong-b"},
	}
	nodes[1].mu.Unlock()
	nodes[0].mu.Lock()
	nodes[0].nextIndex[1] = 3
	nodes[0].mu.Unlock()

	nodes[0].replicate() // round 1: rejected, nextIndex[1] backs off to 2
	nodes[0].mu.Lock()
	afterFirstRound := nodes[0].nextIndex[1]
	nodes[0].mu.Unlock()
	if afterFirstRound != 2 {
		t.Fatalf("nextIndex[1] after the first (rejected) round = %d, want 2 (backed off by one)", afterFirstRound)
	}

	nodes[0].replicate() // round 2: PrevLogIndex=1 now matches — accepted, catches node1 all the way up

	nodes[0].mu.Lock()
	leaderLog := append([]LogEntry(nil), nodes[0].log...)
	nodes[0].mu.Unlock()
	nodes[1].mu.Lock()
	followerLog := append([]LogEntry(nil), nodes[1].log...)
	nodes[1].mu.Unlock()

	if !reflect.DeepEqual(followerLog, leaderLog) {
		t.Fatalf("node1 log after convergence = %+v, want it to match the leader's log %+v", followerLog, leaderLog)
	}

	nodes[0].mu.Lock()
	matchIndex1 := nodes[0].matchIndex[1]
	nextIndex1 := nodes[0].nextIndex[1]
	nodes[0].mu.Unlock()
	if matchIndex1 != 3 {
		t.Fatalf("matchIndex[1] after convergence = %d, want 3", matchIndex1)
	}
	if nextIndex1 != 4 {
		t.Fatalf("nextIndex[1] after convergence = %d, want 4", nextIndex1)
	}
}
