package raft

import "testing"

// TestRequestVoteRejectsStaleTerm proves a voter refuses a candidate
// running an older term than its own, and doesn't change its own term to
// match (that would be backwards — terms only ever move forward).
func TestRequestVoteRejectsStaleTerm(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	r.mu.Lock()
	r.currentTerm = 5
	r.mu.Unlock()

	var reply RequestVoteReply
	if err := r.RequestVote(&RequestVoteArgs{Term: 3, CandidateID: 1}, &reply); err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if reply.VoteGranted {
		t.Fatal("expected vote rejected for a candidate running an older term")
	}
	if reply.Term != 5 {
		t.Fatalf("reply.Term = %d, want 5 (voter's own term, unchanged)", reply.Term)
	}
}

// TestRequestVoteGrantsOncePerTerm proves the one-vote-per-term rule: a
// second, different candidate is refused, but the SAME candidate asking
// again (e.g. a retried RPC after a dropped reply) still gets granted —
// that's idempotency, not a second vote.
func TestRequestVoteGrantsOncePerTerm(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())

	var reply1 RequestVoteReply
	if err := r.RequestVote(&RequestVoteArgs{Term: 1, CandidateID: 1}, &reply1); err != nil {
		t.Fatalf("RequestVote (first): %v", err)
	}
	if !reply1.VoteGranted {
		t.Fatal("expected first vote request granted")
	}

	var reply2 RequestVoteReply
	if err := r.RequestVote(&RequestVoteArgs{Term: 1, CandidateID: 2}, &reply2); err != nil {
		t.Fatalf("RequestVote (second, different candidate): %v", err)
	}
	if reply2.VoteGranted {
		t.Fatal("expected second vote (different candidate, same term) rejected")
	}

	var reply3 RequestVoteReply
	if err := r.RequestVote(&RequestVoteArgs{Term: 1, CandidateID: 1}, &reply3); err != nil {
		t.Fatalf("RequestVote (retry, same candidate): %v", err)
	}
	if !reply3.VoteGranted {
		t.Fatal("expected a retried vote request from the same candidate to still be granted")
	}
}

// TestRequestVoteRejectsOutOfDateLog proves the §5.4.1 safety check: a
// candidate whose log is behind the voter's must never receive a vote,
// even if term/one-vote-per-term rules would otherwise allow it — this is
// what stops a node with a stale log from ever losing committed entries
// by becoming leader.
func TestRequestVoteRejectsOutOfDateLog(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	r.mu.Lock()
	r.log = []LogEntry{{Term: 1}, {Term: 2}} // voter's log: lastIndex=2, lastTerm=2
	r.mu.Unlock()

	var reply RequestVoteReply
	// Candidate's last entry has an older term (1 < 2) — behind, must be refused.
	args := &RequestVoteArgs{Term: 1, CandidateID: 1, LastLogIndex: 1, LastLogTerm: 1}
	if err := r.RequestVote(args, &reply); err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if reply.VoteGranted {
		t.Fatal("expected vote rejected — candidate's log is less up-to-date")
	}
}

// TestRequestVoteGrantsWhenLogAtLeastAsUpToDate is the positive case of
// the same rule: equal last-entry term and equal (or greater) length is
// "at least as up-to-date," which must be granted.
func TestRequestVoteGrantsWhenLogAtLeastAsUpToDate(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	r.mu.Lock()
	r.log = []LogEntry{{Term: 1}}
	r.mu.Unlock()

	var reply RequestVoteReply
	args := &RequestVoteArgs{Term: 1, CandidateID: 1, LastLogIndex: 1, LastLogTerm: 1}
	if err := r.RequestVote(args, &reply); err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if !reply.VoteGranted {
		t.Fatal("expected vote granted — candidate's log is at least as up-to-date")
	}
}

// TestElectionSingleLeader is Day 4's headline requirement: with 3 nodes,
// no faults, one candidate reaches a majority and becomes Leader; the
// other two record their vote and stay Followers.
func TestElectionSingleLeader(t *testing.T) {
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
		t.Fatalf("candidate state after uncontested election = %s, want Leader", got)
	}
	for _, id := range []int{1, 2} {
		if got := nodes[id].State(); got != Follower {
			t.Fatalf("voter %d state = %s, want Follower", id, got)
		}
		nodes[id].mu.Lock()
		votedFor := nodes[id].votedFor
		nodes[id].mu.Unlock()
		if votedFor != 0 {
			t.Fatalf("voter %d votedFor = %d, want 0 (the elected candidate)", id, votedFor)
		}
	}
}

// TestElectionStepsDownOnHigherTerm proves a candidate that discovers a
// higher term via an RPC reply immediately steps down to Follower and
// adopts that term, instead of continuing to count votes for an election
// that's already behind.
func TestElectionStepsDownOnHigherTerm(t *testing.T) {
	transport := NewFakeTransport()
	candidate := NewRaft(0, []int{1}, transport)
	transport.Register(0, candidate)
	peer := NewRaft(1, []int{0}, transport)
	transport.Register(1, peer)

	peer.mu.Lock()
	peer.currentTerm = 10
	peer.mu.Unlock()

	if err := candidate.BecomeCandidate(); err != nil { // candidate's term becomes 1
		t.Fatalf("BecomeCandidate: %v", err)
	}
	candidate.startElection()

	if got := candidate.State(); got != Follower {
		t.Fatalf("state after seeing a higher term in a reply = %s, want Follower", got)
	}
	if got := candidate.Term(); got != 10 {
		t.Fatalf("term after stepping down = %d, want 10 (adopted from the peer's reply)", got)
	}
}

// TestElectionNoMajorityStaysCandidate proves a candidate that can't
// reach a majority (one peer already voted for someone else, two peers
// unreachable) stays a Candidate rather than incorrectly becoming Leader.
func TestElectionNoMajorityStaysCandidate(t *testing.T) {
	transport := NewFakeTransport()
	candidate := NewRaft(0, []int{1, 2, 3}, transport)
	transport.Register(0, candidate)

	peer1 := NewRaft(1, []int{0, 2, 3}, transport)
	transport.Register(1, peer1)
	// peers 2 and 3 are deliberately never registered — CallRequestVote to
	// them errors, simulating unreachable nodes.

	if err := candidate.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	term := candidate.Term()

	// peer1 already committed its vote this term to someone else.
	peer1.mu.Lock()
	peer1.currentTerm = term
	peer1.votedFor = 99
	peer1.mu.Unlock()

	candidate.startElection()

	// Cluster of 4 (0,1,2,3): majority is 3. Votes: self=1, peer1=refused,
	// peers 2/3=unreachable. Total 1 < 3 — must not win.
	if got := candidate.State(); got != Candidate {
		t.Fatalf("state after election with no majority = %s, want unchanged Candidate", got)
	}
}

// TestElectionEndToEndViaTimer wires 3 nodes' real election timers
// together (no manual BecomeCandidate/startElection calls) and proves the
// whole Day 3 + Day 4 machinery converges on exactly one leader — the
// end-to-end version of "get a single leader elected among 3 nodes with
// no faults" from 01-raft/TASKS.md.
func TestElectionEndToEndViaTimer(t *testing.T) {
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
	}
	defer func() {
		for _, n := range nodes {
			n.StopElectionTimer()
		}
	}()

	waitFor(t, 20*ElectionTimeoutMax, func() bool {
		leaders := 0
		for _, n := range nodes {
			if n.State() == Leader {
				leaders++
			}
		}
		return leaders == 1
	})
}
