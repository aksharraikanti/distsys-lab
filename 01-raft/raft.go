// Package raft is a from-scratch implementation of the Raft consensus
// protocol, built as Stage 1 of the distsys-lab learning track. See
// 01-raft/README.md for concept notes and 01-raft/TASKS.md for the
// day-by-day build plan this file follows.
package raft

import (
	"sync"
	"time"
)

// Election/heartbeat timeouts are tunable constants, deliberately kept
// small (10-50ms) rather than production-scale (150-300ms+). This is the
// same choice MIT 6.5840's labrpc-based Raft labs make: it's what keeps
// Stage 1 Day 12's fault-injection suite running in seconds instead of
// minutes. See 01-raft/TASKS.md Day 1.
const (
	ElectionTimeoutMin = 10 * time.Millisecond
	ElectionTimeoutMax = 50 * time.Millisecond
	HeartbeatInterval  = 10 * time.Millisecond
)

// Raft holds one node's state. Day 1 scope was just enough of this struct
// to answer RPCs; Day 2 adds the Follower/Candidate/Leader state machine
// (state.go). Real election and log replication logic land on Days 3-10.
//
// mu protects every field below it — this is Day 2's founding invariant,
// proven by TestConcurrentStateAccess in state_test.go (RPC handlers and
// state transitions firing concurrently, clean under `go test -race`).
type Raft struct {
	mu sync.Mutex

	id        int
	peers     []int
	transport Transport

	state       serverState
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int
}

// NewRaft constructs a node with the given id, its peer ids, and the
// transport it should use to reach them. Every node starts as a Follower
// with votedFor at -1 (no peer id is negative), meaning "hasn't voted this
// term."
func NewRaft(id int, peers []int, transport Transport) *Raft {
	return &Raft{
		id:        id,
		peers:     peers,
		transport: transport,
		state:     Follower,
		votedFor:  -1,
	}
}

// RequestVote is a stub RPC handler for Day 1 — it always reports the
// current term and refuses the vote. Real election logic (term
// comparison, log up-to-dateness check) lands Day 4.
func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	reply.Term = r.currentTerm
	reply.VoteGranted = false
	return nil
}

// AppendEntries is a stub RPC handler for Day 1 — it always reports the
// current term and reports failure. Real replication/heartbeat logic
// lands Days 5, 7-10.
func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	reply.Term = r.currentTerm
	reply.Success = false
	return nil
}
