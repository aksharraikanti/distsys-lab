package raft

import "testing"

// TestBecomeFollowerIgnoresLowerTerm proves term monotonicity at the
// primitive level: BecomeFollower called with a term lower than current
// must never move term or votedFor backwards. In practice none of this
// package's own call sites ever invoke it that way — RequestVote and
// AppendEntries both check args.Term >= currentTerm before calling
// becomeFollowerLocked, and startElection/sendHeartbeats both check
// reply.Term > currentTerm — but this is exactly the kind of invariant
// worth guarding at the primitive itself, since a single future call site
// that skips the check would otherwise silently corrupt state.
func TestBecomeFollowerIgnoresLowerTerm(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	r.mu.Lock()
	r.currentTerm = 7
	r.votedFor = 3
	r.mu.Unlock()

	r.BecomeFollower(4)

	r.mu.Lock()
	term, votedFor := r.currentTerm, r.votedFor
	r.mu.Unlock()

	if term != 7 {
		t.Fatalf("term after BecomeFollower(lower term) = %d, want unchanged 7", term)
	}
	if votedFor != 3 {
		t.Fatalf("votedFor after BecomeFollower(lower term) = %d, want unchanged 3", votedFor)
	}
	// The state transition itself still happens — BecomeFollower(term) is
	// "step down," not "ignore this call." Only term/votedFor are
	// protected from moving backwards.
	if got := r.State(); got != Follower {
		t.Fatalf("state after BecomeFollower(lower term) = %s, want Follower", got)
	}
}

// TestSplitVoteRequiresNewElection forces a genuine split vote — two
// candidates in the same term, each with exactly half the cluster's
// votes — and proves neither incorrectly becomes Leader. It then proves
// the standard Raft recovery path: a fresh round (new term) with an
// uncontested candidate succeeds, because a split vote is a temporary
// setback the randomized timeout eventually breaks, not a permanent
// deadlock.
func TestSplitVoteRequiresNewElection(t *testing.T) {
	transport := NewFakeTransport()
	ids := []int{0, 1, 2, 3}
	nodes := make(map[int]*Raft, len(ids))
	for _, id := range ids {
		n := NewRaft(id, otherPeers(ids, id), transport)
		nodes[id] = n
		transport.Register(id, n)
	}

	// node0 and node2 both become candidates for the same term.
	if err := nodes[0].BecomeCandidate(); err != nil {
		t.Fatalf("node0 BecomeCandidate: %v", err)
	}
	if err := nodes[2].BecomeCandidate(); err != nil {
		t.Fatalf("node2 BecomeCandidate: %v", err)
	}
	term := nodes[0].Term()
	if got := nodes[2].Term(); got != term {
		t.Fatalf("node0 and node2 entered different terms (%d vs %d) — test setup assumes they collide on the same term", term, got)
	}

	// Pre-commit the swing voters' votes to force an even 2-2 split in
	// this 4-node cluster (majority is 3): node1 -> node0, node3 -> node2.
	nodes[1].mu.Lock()
	nodes[1].currentTerm = term
	nodes[1].votedFor = 0
	nodes[1].mu.Unlock()

	nodes[3].mu.Lock()
	nodes[3].currentTerm = term
	nodes[3].votedFor = 2
	nodes[3].mu.Unlock()

	nodes[0].startElection()
	nodes[2].startElection()

	if got := nodes[0].State(); got != Candidate {
		t.Fatalf("node0 state after split vote = %s, want unchanged Candidate (only 2/4 votes, majority is 3)", got)
	}
	if got := nodes[2].State(); got != Candidate {
		t.Fatalf("node2 state after split vote = %s, want unchanged Candidate (only 2/4 votes, majority is 3)", got)
	}

	// Recovery: node0 restarts its own election for a fresh, higher term.
	// Every other node is still on the split-vote term, so all of them
	// grant node0's vote this time — no rival candidate is competing for
	// this new term.
	if err := nodes[0].BecomeCandidate(); err != nil {
		t.Fatalf("node0 BecomeCandidate (retry): %v", err)
	}
	nodes[0].startElection()

	if got := nodes[0].State(); got != Leader {
		t.Fatalf("node0 state after retry election = %s, want Leader — a split vote must be recoverable, not a permanent deadlock", got)
	}
}

// TestStaleLeaderStepsDownOnHigherTermReply covers the "stale-leader
// rejection" edge case from the other direction than heartbeat_test.go's
// TestAppendEntriesRejectsStaleTerm: not a follower rejecting a stale
// leader's heartbeat, but the stale leader itself discovering — via its
// own heartbeat replies — that it's been superseded, and stepping down.
// This models a leader that got partitioned away while the rest of the
// cluster moved on to a higher term without it.
func TestStaleLeaderStepsDownOnHigherTermReply(t *testing.T) {
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

	// The rest of the cluster moves on to term 5 without node0 — as if a
	// partition isolated node0 from an election that happened without it.
	for _, id := range []int{1, 2} {
		nodes[id].mu.Lock()
		nodes[id].currentTerm = 5
		nodes[id].mu.Unlock()
	}

	// node0, still believing it's the term-1 leader, sends a heartbeat
	// round — exactly what a real stale leader does once a partition heals.
	nodes[0].sendHeartbeats()

	if got := nodes[0].State(); got != Follower {
		t.Fatalf("stale leader state after replies reveal a higher term = %s, want Follower", got)
	}
	if got := nodes[0].Term(); got != 5 {
		t.Fatalf("stale leader term after stepping down = %d, want 5 (adopted from a follower's reply)", got)
	}
}

// TestNodeRestartRejoinsCluster simulates a crash-and-restart without
// persistence (Day 11 adds that): the restarted node is a brand-new Raft
// instance that has forgotten its term, vote, and log entirely, wired
// back into the cluster under the same peer id. It should still converge
// — hear the current leader's heartbeats, adopt its term, and settle as
// a Follower — and the cluster should never end up with more than one
// leader through the whole process.
//
// This tests liveness/availability through a restart, not safety — a
// restarted node with no memory of its prior term or vote could in
// principle vote for a stale candidate it "shouldn't" remember refusing.
// Closing that gap for real is exactly what Day 11's persistence is for;
// this day only proves the cluster keeps functioning when a node forgets
// everything and rejoins.
func TestNodeRestartRejoinsCluster(t *testing.T) {
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

	// Crash a follower (never the leader — restarting the leader itself
	// is a distinct scenario Day 7+'s log-driven state machine will cover
	// more meaningfully) and replace it with a fresh instance under the
	// same id. Re-registering on the shared FakeTransport is the crash:
	// existing peers keep addressing this id exactly as before, unaware
	// anything happened underneath it.
	var restartID int
	for _, id := range ids {
		if id != leaderID {
			restartID = id
			break
		}
	}
	nodes[restartID].StopElectionTimer()

	fresh := NewRaft(restartID, otherPeers(ids, restartID), transport)
	nodes[restartID] = fresh
	transport.Register(restartID, fresh)
	go fresh.RunElectionTimer()
	go fresh.RunHeartbeats()

	waitFor(t, 20*ElectionTimeoutMax, func() bool {
		return fresh.State() == Follower && fresh.Term() == nodes[leaderID].Term()
	})

	leaders := 0
	for _, n := range nodes {
		if n.State() == Leader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("expected exactly 1 leader after a node restart, got %d", leaders)
	}
}
