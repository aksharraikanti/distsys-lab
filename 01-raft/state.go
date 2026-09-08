package raft

import "fmt"

// serverState is one of the three roles a Raft node can hold at any time.
// The zero value is Follower, which is also every node's starting state —
// see NewRaft.
type serverState int

const (
	Follower serverState = iota
	Candidate
	Leader
)

func (s serverState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return fmt.Sprintf("serverState(%d)", int(s))
	}
}

// State returns the node's current role. Safe to call concurrently.
func (r *Raft) State() serverState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

// Term returns the node's current term. Safe to call concurrently.
func (r *Raft) Term() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm
}

// BecomeFollower transitions the node to Follower for the given term. This
// is the one transition valid from any state — a node steps down the
// moment it sees a term higher than its own, whether it's currently a
// Follower, Candidate, or Leader (Raft paper §5.1: "If RPC request or
// response contains term T > currentTerm, set currentTerm = T, convert to
// follower").
//
// votedFor only resets when the term actually advances — a node that's
// already a Follower in the same term (e.g. after losing an election it
// didn't vote in) must not forget who it voted for.
func (r *Raft) BecomeFollower(term int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.becomeFollowerLocked(term)
}

// becomeFollowerLocked is BecomeFollower's implementation, for callers that
// already hold r.mu (Day 4's election-response handling needs this — it
// can't call the locking BecomeFollower without deadlocking itself).
func (r *Raft) becomeFollowerLocked(term int) {
	if term > r.currentTerm {
		r.currentTerm = term
		r.votedFor = -1
	}
	r.state = Follower
}

// BecomeCandidate starts (or restarts) an election: the node increments
// its term, votes for itself, and transitions to Candidate. Valid from
// Follower (election timeout fired) or Candidate (previous election
// timed out with no winner — Raft paper §5.2 calls this "starting a new
// election"). Invalid from Leader — a leader doesn't nominate itself
// against its own term.
func (r *Raft) BecomeCandidate() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.becomeCandidateLocked()
}

func (r *Raft) becomeCandidateLocked() error {
	if r.state == Leader {
		return fmt.Errorf("raft: invalid transition Leader -> Candidate")
	}
	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.id
	return nil
}

// BecomeLeader transitions the node to Leader after it wins an election.
// Valid only from Candidate — a node can't become leader without having
// first run for election in the current term.
func (r *Raft) BecomeLeader() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.becomeLeaderLocked()
}

func (r *Raft) becomeLeaderLocked() error {
	if r.state != Candidate {
		return fmt.Errorf("raft: invalid transition %s -> Leader (must come from Candidate)", r.state)
	}
	r.state = Leader
	return nil
}
