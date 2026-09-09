package raft

import (
	"testing"
	"time"
)

// TestAppendEntriesRejectsStaleTerm proves a node refuses a heartbeat
// from a leader running an older term than its own — the mirror of
// TestRequestVoteRejectsStaleTerm for the other RPC.
func TestAppendEntriesRejectsStaleTerm(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	r.mu.Lock()
	r.currentTerm = 5
	r.mu.Unlock()

	var reply AppendEntriesReply
	if err := r.AppendEntries(&AppendEntriesArgs{Term: 3, LeaderID: 1}, &reply); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if reply.Success {
		t.Fatal("expected heartbeat rejected from a leader running an older term")
	}
	if reply.Term != 5 {
		t.Fatalf("reply.Term = %d, want 5 (this node's own term, unchanged)", reply.Term)
	}
}

// TestAppendEntriesStepsDownCandidate proves a Candidate that hears a
// same-term AppendEntries from a legitimate leader steps down to
// Follower — Raft paper §5.2's "Rules for Servers, Candidates" bullet 3.
// The term doesn't advance here, so votedFor (the Candidate's self-vote)
// must be preserved, not reset.
func TestAppendEntriesStepsDownCandidate(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	if err := r.BecomeCandidate(); err != nil { // term becomes 1, votes for self
		t.Fatalf("BecomeCandidate: %v", err)
	}
	term := r.Term()

	var reply AppendEntriesReply
	if err := r.AppendEntries(&AppendEntriesArgs{Term: term, LeaderID: 1}, &reply); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !reply.Success {
		t.Fatal("expected same-term heartbeat from a legitimate leader to succeed")
	}
	if got := r.State(); got != Follower {
		t.Fatalf("state after same-term AppendEntries while Candidate = %s, want Follower", got)
	}
	if got := r.Term(); got != term {
		t.Fatalf("term after stepping down (same term) = %d, want unchanged %d", got, term)
	}

	r.mu.Lock()
	votedFor := r.votedFor
	r.mu.Unlock()
	if votedFor != 0 {
		t.Fatalf("votedFor after same-term step-down = %d, want unchanged 0 (preserved, term didn't advance)", votedFor)
	}
}

// TestAppendEntriesAdvancesTermAndBecomesFollower proves a Leader that
// hears from a leader in a strictly newer term steps down, adopts the
// new term, and resets votedFor — the mirror of
// TestElectionStepsDownOnHigherTerm for the other RPC.
func TestAppendEntriesAdvancesTermAndBecomesFollower(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	if err := r.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := r.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}

	var reply AppendEntriesReply
	if err := r.AppendEntries(&AppendEntriesArgs{Term: 5, LeaderID: 1}, &reply); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if !reply.Success {
		t.Fatal("expected a newer-term AppendEntries to succeed")
	}
	if got := r.State(); got != Follower {
		t.Fatalf("state after newer-term AppendEntries while Leader = %s, want Follower", got)
	}
	if got := r.Term(); got != 5 {
		t.Fatalf("term after stepping down = %d, want 5 (adopted from the leader)", got)
	}

	r.mu.Lock()
	votedFor := r.votedFor
	r.mu.Unlock()
	if votedFor != -1 {
		t.Fatalf("votedFor after a term advance = %d, want -1 (reset — this is a new term)", votedFor)
	}
}

// TestAppendEntriesResetsElectionTimer proves the follower half of Day
// 5's requirement directly through the real RPC path (not the raw
// ResetElectionTimer channel Day 3 tested): a node that keeps receiving
// AppendEntries faster than its election timeout never starts an
// election.
func TestAppendEntriesResetsElectionTimer(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	go r.RunElectionTimer()
	defer r.StopElectionTimer()

	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(ElectionTimeoutMin / 4)
		defer ticker.Stop()
		term := 1
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				var reply AppendEntriesReply
				_ = r.AppendEntries(&AppendEntriesArgs{Term: term, LeaderID: 1}, &reply)
			}
		}
	}()

	time.Sleep(5 * ElectionTimeoutMax)
	close(stop)

	if got := r.State(); got != Follower {
		t.Fatalf("state after sustained AppendEntries = %s, want Follower (should never have timed out)", got)
	}
}

// TestHeartbeatsKeepLeaderStable is Day 5's headline end-to-end proof:
// once a leader is elected, heartbeats keep every follower's election
// timer reset, so leadership stays stable through many election-timeout
// cycles — unlike Day 4, where nothing yet stopped a follower from
// spuriously starting a competing election.
func TestHeartbeatsKeepLeaderStable(t *testing.T) {
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
	}
	defer func() {
		for _, n := range nodes {
			n.StopElectionTimer()
		}
	}()

	leaderID := -1
	waitFor(t, 20*ElectionTimeoutMax, func() bool {
		leaders := 0
		for id, n := range nodes {
			if n.State() == Leader {
				leaders++
				leaderID = id
			}
		}
		return leaders == 1
	})
	leaderTerm := nodes[leaderID].Term()

	// A sustained window covering many election-timeout cycles. With
	// heartbeats resetting followers' timers, leadership must not churn.
	time.Sleep(15 * ElectionTimeoutMax)

	if got := nodes[leaderID].State(); got != Leader {
		t.Fatalf("original leader (node %d) state = %s after sustained heartbeats, want unchanged Leader", leaderID, got)
	}
	if got := nodes[leaderID].Term(); got != leaderTerm {
		t.Fatalf("leader term changed from %d to %d during a stable heartbeat window — leadership churned", leaderTerm, got)
	}

	leaders := 0
	for _, n := range nodes {
		if n.State() == Leader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("expected exactly 1 leader after sustained heartbeats, got %d", leaders)
	}
}
