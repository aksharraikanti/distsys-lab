package raft

import (
	"reflect"
	"testing"
)

// TestProposeRejectsWhenNotLeader proves a Follower (or Candidate) can't
// accept a client command — only a Leader can, so callers know they need
// to find and retry against the real leader.
func TestProposeRejectsWhenNotLeader(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())

	index, term, isLeader := r.Propose("hello")
	if isLeader {
		t.Fatal("expected Propose to reject on a non-leader Follower")
	}
	if index != 0 || term != 0 {
		t.Fatalf("Propose on non-leader = (index=%d, term=%d), want (0, 0)", index, term)
	}

	r.mu.Lock()
	logLen := len(r.log)
	r.mu.Unlock()
	if logLen != 0 {
		t.Fatalf("log length after rejected Propose = %d, want 0 (nothing should have been appended)", logLen)
	}
}

// TestProposeAppendsToLeaderLog proves the Day 7 headline requirement:
// a Leader accepting a client command appends it to its own log and
// returns the index/term it was assigned. Indices are 1-indexed and
// sequential, matching lastLogInfoLocked's convention from Day 4.
func TestProposeAppendsToLeaderLog(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	if err := r.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := r.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	term := r.Term()

	index1, term1, isLeader1 := r.Propose("set x=1")
	if !isLeader1 {
		t.Fatal("expected Propose to succeed on a Leader")
	}
	if index1 != 1 || term1 != term {
		t.Fatalf("first Propose = (index=%d, term=%d), want (1, %d)", index1, term1, term)
	}

	index2, term2, isLeader2 := r.Propose("set x=2")
	if !isLeader2 {
		t.Fatal("expected second Propose to succeed on a Leader")
	}
	if index2 != 2 || term2 != term {
		t.Fatalf("second Propose = (index=%d, term=%d), want (2, %d)", index2, term2, term)
	}

	r.mu.Lock()
	log := append([]LogEntry(nil), r.log...)
	r.mu.Unlock()

	want := []LogEntry{
		{Term: term, Command: "set x=1"},
		{Term: term, Command: "set x=2"},
	}
	if !reflect.DeepEqual(log, want) {
		t.Fatalf("leader log = %+v, want %+v", log, want)
	}
}

// TestAppendEntriesAppendsRealEntries proves the follower side of Day 7:
// an AppendEntries call carrying real entries (not just an empty
// heartbeat) actually lands them in the receiving node's log, starting
// right after PrevLogIndex.
func TestAppendEntriesAppendsRealEntries(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())

	entries := []LogEntry{
		{Term: 1, Command: "a"},
		{Term: 1, Command: "b"},
	}
	args := &AppendEntriesArgs{Term: 1, LeaderID: 1, PrevLogIndex: 0, Entries: entries}

	var reply AppendEntriesReply
	if err := r.AppendEntries(args, &reply); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !reply.Success {
		t.Fatal("expected AppendEntries with real entries to succeed")
	}

	r.mu.Lock()
	log := append([]LogEntry(nil), r.log...)
	r.mu.Unlock()
	if !reflect.DeepEqual(log, entries) {
		t.Fatalf("follower log after AppendEntries = %+v, want %+v", log, entries)
	}
}

// TestAppendEntriesOverwritesFromPrevLogIndex proves a follower's log
// gets overwritten from PrevLogIndex onward when new entries arrive
// there — the naive "trust PrevLogIndex" version of what Day 10's real
// consistency check will later guard properly.
func TestAppendEntriesOverwritesFromPrevLogIndex(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	r.mu.Lock()
	r.log = []LogEntry{
		{Term: 1, Command: "old-1"},
		{Term: 1, Command: "old-2"},
		{Term: 1, Command: "old-3"},
	}
	r.mu.Unlock()

	newEntries := []LogEntry{{Term: 2, Command: "new-2"}}
	args := &AppendEntriesArgs{Term: 2, LeaderID: 1, PrevLogIndex: 1, Entries: newEntries}

	var reply AppendEntriesReply
	if err := r.AppendEntries(args, &reply); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !reply.Success {
		t.Fatal("expected AppendEntries to succeed")
	}

	r.mu.Lock()
	log := append([]LogEntry(nil), r.log...)
	r.mu.Unlock()

	want := []LogEntry{
		{Term: 1, Command: "old-1"}, // kept: index 1, at or before PrevLogIndex
		{Term: 2, Command: "new-2"}, // everything after PrevLogIndex is replaced
	}
	if !reflect.DeepEqual(log, want) {
		t.Fatalf("follower log after overwrite = %+v, want %+v", log, want)
	}
}

// TestAppendEntriesEmptyEntriesLeavesLogUntouched proves a plain
// heartbeat (no entries) never modifies an existing log — Day 7 must not
// regress Day 5's heartbeat behavior.
func TestAppendEntriesEmptyEntriesLeavesLogUntouched(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	r.mu.Lock()
	r.log = []LogEntry{{Term: 1, Command: "keep-me"}}
	r.mu.Unlock()

	var reply AppendEntriesReply
	args := &AppendEntriesArgs{Term: 1, LeaderID: 1} // Entries is nil — a heartbeat
	if err := r.AppendEntries(args, &reply); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !reply.Success {
		t.Fatal("expected heartbeat to succeed")
	}

	r.mu.Lock()
	log := append([]LogEntry(nil), r.log...)
	r.mu.Unlock()
	want := []LogEntry{{Term: 1, Command: "keep-me"}}
	if !reflect.DeepEqual(log, want) {
		t.Fatalf("log after an empty-entries heartbeat = %+v, want unchanged %+v", log, want)
	}
}
