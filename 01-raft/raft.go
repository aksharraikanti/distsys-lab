// Package raft is a from-scratch implementation of the Raft consensus
// protocol, built as Stage 1 of the distsys-lab learning track. See
// 01-raft/README.md for concept notes and 01-raft/TASKS.md for the
// day-by-day build plan this file follows.
package raft

import (
	"math/rand"
	"sync"
	"time"
)

// Election/heartbeat timeouts are tunable constants, deliberately kept
// small (10-50ms) rather than production-scale (150-300ms+). This is the
// same choice MIT 6.5840's labrpc-based Raft labs make: it's what keeps
// Stage 1 Day 12's fault-injection suite running in seconds instead of
// minutes. See 01-raft/TASKS.md Day 1.
//
// HeartbeatInterval must sit well below ElectionTimeoutMin — the standard
// Raft guidance is broadcastTime << electionTimeout, usually by 5-10x —
// or a follower can legitimately time out and start an election before
// its next heartbeat was even due to arrive. It only became load-bearing
// once Day 5 wired heartbeats up to actually reset followers' timers;
// before that, nothing depended on the gap between the two.
const (
	ElectionTimeoutMin = 10 * time.Millisecond
	ElectionTimeoutMax = 50 * time.Millisecond
	HeartbeatInterval  = 2 * time.Millisecond
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

	// Election-timer machinery (Day 3, election_timer.go). resetElectionTimer
	// is buffered so ResetElectionTimer never blocks its caller — a dropped
	// reset just means the current countdown runs a little longer, which is
	// harmless; a blocked RPC handler waiting on channel space is not.
	resetElectionTimer chan struct{}
	stopCh             chan struct{}
	stopOnce           sync.Once

	// rng is per-node (not the global math/rand source) so concurrently
	// created nodes don't share timing state, and rngMu protects it since
	// *rand.Rand is not safe for concurrent use.
	rng   *rand.Rand
	rngMu sync.Mutex
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

		resetElectionTimer: make(chan struct{}, 1),
		stopCh:             make(chan struct{}),
		rng:                rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(id))),
	}
}

// RequestVote handles an incoming vote request (Raft paper §5.2, §5.4).
// A vote is granted only if all of: the candidate's term is at least as
// current as this node's, this node hasn't already voted for someone else
// this term, and the candidate's log is at least as up-to-date as this
// node's (candidateLogIsUpToDateLocked) — that last check is what stops a
// node with a stale, incomplete log from ever becoming leader and losing
// committed entries.
func (r *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term > r.currentTerm {
		r.becomeFollowerLocked(args.Term)
	}
	reply.Term = r.currentTerm

	if args.Term < r.currentTerm {
		reply.VoteGranted = false
		return nil
	}

	alreadyVotedForSomeoneElse := r.votedFor != -1 && r.votedFor != args.CandidateID
	logIsUpToDate := r.candidateLogIsUpToDateLocked(args.LastLogIndex, args.LastLogTerm)

	if alreadyVotedForSomeoneElse || !logIsUpToDate {
		reply.VoteGranted = false
		return nil
	}

	r.votedFor = args.CandidateID
	reply.VoteGranted = true
	// Granting a vote means this node just heard from a legitimate,
	// at-least-as-current candidate — that resets how long it waits before
	// starting its own election. ResetElectionTimer only touches a channel,
	// never r.mu, so calling it while still holding the lock is safe.
	r.ResetElectionTimer()
	return nil
}

// AppendEntries handles an incoming heartbeat or log-replication call
// (Raft paper §5.2, §5.3). Day 5 scope is heartbeats only — Entries is
// always empty until Day 7, so the prevLogIndex/prevLogTerm consistency
// check that real replication needs lands Day 10; for now, any
// term-valid call just succeeds.
//
// A term-valid AppendEntries (args.Term >= currentTerm) is proof a
// legitimate leader exists for that term: a Candidate steps down (§5.2,
// "Rules for Servers" — Candidates, bullet 3), and this node resets its
// election timer either way. becomeFollowerLocked only resets votedFor
// when the term actually advances, so calling it even when
// args.Term == currentTerm is safe — it won't discard an in-progress
// term's vote.
func (r *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		reply.Success = false
		return nil
	}

	r.becomeFollowerLocked(args.Term)
	reply.Term = r.currentTerm
	reply.Success = true
	r.ResetElectionTimer()
	return nil
}
