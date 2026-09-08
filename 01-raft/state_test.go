package raft

import (
	"sync"
	"testing"
)

// TestStateTransitions proves the state machine transitions correctly
// under manual calls — Day 2's bar. No election timeout or RPC-driven
// logic is exercised here; each transition method is called directly.
func TestStateTransitions(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())

	if got := r.State(); got != Follower {
		t.Fatalf("new node state = %s, want Follower", got)
	}

	// Follower -> Candidate: starts an election, term advances, votes for self.
	if err := r.BecomeCandidate(); err != nil {
		t.Fatalf("Follower -> Candidate: %v", err)
	}
	if got := r.State(); got != Candidate {
		t.Fatalf("state after BecomeCandidate = %s, want Candidate", got)
	}
	if got := r.Term(); got != 1 {
		t.Fatalf("term after first BecomeCandidate = %d, want 1", got)
	}

	// Candidate -> Candidate: election timed out with no winner, restart.
	if err := r.BecomeCandidate(); err != nil {
		t.Fatalf("Candidate -> Candidate (restart): %v", err)
	}
	if got := r.Term(); got != 2 {
		t.Fatalf("term after second BecomeCandidate = %d, want 2", got)
	}

	// Candidate -> Leader: won the election.
	if err := r.BecomeLeader(); err != nil {
		t.Fatalf("Candidate -> Leader: %v", err)
	}
	if got := r.State(); got != Leader {
		t.Fatalf("state after BecomeLeader = %s, want Leader", got)
	}

	// Leader -> Follower: discovered a higher term.
	r.BecomeFollower(5)
	if got := r.State(); got != Follower {
		t.Fatalf("state after BecomeFollower = %s, want Follower", got)
	}
	if got := r.Term(); got != 5 {
		t.Fatalf("term after BecomeFollower(5) = %d, want 5", got)
	}
}

// TestInvalidTransitions proves the rules that must be rejected: a Leader
// can't nominate itself candidate, and Leader can only be reached from
// Candidate.
func TestInvalidTransitions(t *testing.T) {
	t.Run("Follower cannot become Leader directly", func(t *testing.T) {
		r := NewRaft(0, []int{1, 2}, NewFakeTransport())
		if err := r.BecomeLeader(); err == nil {
			t.Fatal("expected error transitioning Follower -> Leader, got nil")
		}
		if got := r.State(); got != Follower {
			t.Fatalf("state after rejected transition = %s, want unchanged Follower", got)
		}
	})

	t.Run("Leader cannot become Candidate", func(t *testing.T) {
		r := NewRaft(0, []int{1, 2}, NewFakeTransport())
		if err := r.BecomeCandidate(); err != nil {
			t.Fatalf("Follower -> Candidate: %v", err)
		}
		if err := r.BecomeLeader(); err != nil {
			t.Fatalf("Candidate -> Leader: %v", err)
		}
		if err := r.BecomeCandidate(); err == nil {
			t.Fatal("expected error transitioning Leader -> Candidate, got nil")
		}
		if got := r.State(); got != Leader {
			t.Fatalf("state after rejected transition = %s, want unchanged Leader", got)
		}
	})
}

// TestBecomeFollowerPreservesVoteWithinSameTerm proves votedFor is only
// reset when the term actually advances — a node that steps down to
// Follower without the term changing (e.g. it lost an election as a
// Candidate) must not forget who it already voted for this term.
func TestBecomeFollowerPreservesVoteWithinSameTerm(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())

	r.mu.Lock()
	r.currentTerm = 3
	r.votedFor = 1
	r.mu.Unlock()

	r.BecomeFollower(3) // same term — must not reset votedFor

	r.mu.Lock()
	got := r.votedFor
	r.mu.Unlock()
	if got != 1 {
		t.Fatalf("votedFor after BecomeFollower(same term) = %d, want unchanged 1", got)
	}
}

// TestConcurrentStateAccess is the eng-review-required race test: it fires
// RPC handlers and state transitions (standing in for Day 3's election
// timer, which doesn't exist yet) at the same Raft instance concurrently.
// The single mutex established in raft.go must make this clean under
// `go test -race` — that invariant, not any particular election outcome,
// is what this test proves.
func TestConcurrentStateAccess(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())

	var wg sync.WaitGroup

	// Stands in for Day 3's election timer repeatedly firing.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = r.BecomeCandidate()
		}
	}()

	// Stands in for concurrent incoming RPCs from peers.
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				var rvReply RequestVoteReply
				_ = r.RequestVote(&RequestVoteArgs{Term: 1, CandidateID: 1}, &rvReply)

				var aeReply AppendEntriesReply
				_ = r.AppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: 1}, &aeReply)

				_ = r.State()
				_ = r.Term()
			}
		}()
	}

	wg.Wait()
}
