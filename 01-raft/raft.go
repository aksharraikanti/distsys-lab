// Package raft is a from-scratch implementation of the Raft consensus
// protocol, built as Stage 1 of the distsys-lab learning track. See
// 01-raft/README.md for concept notes and 01-raft/TASKS.md for the
// day-by-day build plan this file follows.
package raft

import (
	"fmt"
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
//
// ElectionTimeoutMin sits at 10x HeartbeatInterval rather than the 5x this
// started at: `go test ./...` runs 01-raft and 02-kv-store's -race binaries
// concurrently, and under that CPU contention a single heartbeat can be
// scheduler-delayed close enough to a 5x floor to trip a spurious election
// (see TestHeartbeatsKeepLeaderStable flakiness). 10x buys headroom against
// that jitter. Only the floor moves — ElectionTimeoutMax stays put so the
// many `N * ElectionTimeoutMax` wait windows elsewhere in the suite don't
// grow and drag other tests further into that same contention window.
const (
	ElectionTimeoutMin = 20 * time.Millisecond
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

	// persister durably stores currentTerm/votedFor/log (persist.go). Nil
	// by default (NewRaft) — a node with no persister is a pure in-memory
	// node, which is what most tests want. NewRaftWithPersister wires
	// one in for tests/uses that specifically exercise restart recovery.
	persister Persister

	state       serverState
	currentTerm int
	votedFor    int
	log         []LogEntry

	// lastIncludedIndex/lastIncludedTerm describe the entry a snapshot
	// (Day 6, snapshot.go) most recently replaced: r.log now holds only
	// entries AFTER lastIncludedIndex, so every absolute (paper-style,
	// 1-indexed) log index has to be translated to a position in r.log
	// via physicalIndexLocked before it can be used to index the slice
	// directly. lastIncludedIndex 0 (the default) means "no snapshot has
	// ever been taken" — every absolute index still equals its physical
	// one, exactly as it did before Day 6.
	lastIncludedIndex int
	lastIncludedTerm  int

	// snapshotData is this node's current snapshot bytes, kept in
	// memory regardless of whether a persister is attached. A persister
	// (or its absence) only governs whether this survives a RESTART —
	// but Day 7's InstallSnapshot needs a live, in-memory node (leader
	// or, later, a re-sharing follower) to be able to hand its current
	// snapshot to a peer over RPC at any moment, which has nothing to
	// do with restart/persistence at all. Tying "can I read my own
	// snapshot back" to "do I have a persister" — true for Day 6's own
	// restore-on-restart use, since a persister-less node genuinely has
	// nothing to recover — would silently break replication for the
	// (very common in this project's own tests) persister-less case.
	snapshotData []byte

	commitIndex int
	lastApplied int

	// pendingSnapshot holds a just-installed snapshot (Day 7) waiting to
	// be handed to the state machine via ApplyCh. It's delivered by
	// applyPending's own goroutine (RunApplyLoop), NOT sent directly by
	// InstallSnapshot's RPC-handler goroutine — ApplyCh must only ever
	// have ONE sender, or a regular committed entry and an installed
	// snapshot could race each other onto the channel in the wrong
	// order (e.g. the state machine seeing a stale entry applied AFTER
	// a newer snapshot already superseded it). Routing snapshot
	// delivery through the same single apply loop that already
	// delivers every regular entry preserves that invariant.
	pendingSnapshot *ApplyMsg

	// Leader-only replication state (Raft paper Figure 2, "reinitialized
	// after election" — see becomeLeaderLocked). Both are keyed by peer
	// id, not by cluster position, so a peer's entry stays meaningful
	// even as other peers come and go.
	//
	// nextIndex[p]: the next log index this leader will try sending to
	// peer p. Starts optimistically at (this leader's last log index + 1)
	// — "assume p is fully caught up" — and gets walked backward by
	// replicateToPeer on a rejection.
	//
	// matchIndex[p]: the highest log index this leader has *confirmed*
	// (via a successful reply) is replicated on peer p. Starts at 0 — "no
	// confirmation yet." advanceCommitIndexLocked (commit.go) uses this to
	// know when an entry has reached a majority.
	nextIndex  map[int]int
	matchIndex map[int]int

	// ApplyCh delivers each committed entry, in order, exactly once, as
	// commitIndex advances past lastApplied — see RunApplyLoop (apply.go).
	// Every node (not just the leader) applies from its own log/commitIndex,
	// which is what makes this the channel a state machine on ANY node —
	// leader or follower — reads from.
	ApplyCh chan ApplyMsg

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

// newRaft is the shared constructor NewRaft and NewRaftWithPersister both
// build on. If persister is non-nil, restoreLocked runs before returning
// — recovering any previously-persisted currentTerm/votedFor/log before
// the node does anything else, which is what makes a freshly constructed
// Raft against the SAME persister instance behave like a real restart.
func newRaft(id int, peers []int, transport Transport, persister Persister) *Raft {
	r := &Raft{
		id:        id,
		peers:     peers,
		transport: transport,
		persister: persister,
		state:     Follower,
		votedFor:  -1,

		ApplyCh: make(chan ApplyMsg, 64),

		resetElectionTimer: make(chan struct{}, 1),
		stopCh:             make(chan struct{}),
		rng:                rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(id))),
	}
	r.restoreLocked()
	return r
}

// NewRaft constructs a node with the given id, its peer ids, and the
// transport it should use to reach them. Every node starts as a Follower
// with votedFor at -1 (no peer id is negative), meaning "hasn't voted this
// term." No persister is attached — this node's state lives only in
// memory, exactly as it has since Day 1. Suitable for the vast majority
// of tests, which don't exercise crash/restart behavior.
func NewRaft(id int, peers []int, transport Transport) *Raft {
	return newRaft(id, peers, transport, nil)
}

// NewRaftWithPersister constructs a node whose currentTerm, votedFor, and
// log survive a restart via persister: every mutation to those three
// fields is durably written before it becomes visible outside this node,
// and construction itself recovers any existing state before returning.
// Passing the SAME persister instance to a freshly constructed Raft
// after discarding the old one is exactly what simulates "restart a
// crashed node" in tests — see persist_test.go.
func NewRaftWithPersister(id int, peers []int, transport Transport, persister Persister) *Raft {
	return newRaft(id, peers, transport, persister)
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
	// votedFor just changed again after becomeFollowerLocked's own
	// persist above (or this node was already at args.Term, so that
	// persist never ran at all) — either way, this specific mutation
	// needs its own persist before the vote is granted in the reply.
	r.persistLocked()
	reply.VoteGranted = true
	// Granting a vote means this node just heard from a legitimate,
	// at-least-as-current candidate — that resets how long it waits before
	// starting its own election. ResetElectionTimer only touches a channel,
	// never r.mu, so calling it while still holding the lock is safe.
	r.ResetElectionTimer()
	return nil
}

// AppendEntries handles an incoming heartbeat or log-replication call
// (Raft paper §5.2, §5.3).
//
// A term-valid AppendEntries (args.Term >= currentTerm) is proof a
// legitimate leader exists for that term: a Candidate steps down (§5.2,
// "Rules for Servers" — Candidates, bullet 3), and this node resets its
// election timer either way. becomeFollowerLocked only resets votedFor
// when the term actually advances, so calling it even when
// args.Term == currentTerm is safe — it won't discard an in-progress
// term's vote.
//
// Day 10's log consistency check (§5.3) runs next: this node must
// already have an entry at PrevLogIndex whose term matches PrevLogTerm,
// or the leader's entries don't causally attach to anything real in this
// node's log and must be refused. PrevLogIndex == 0 always passes — it
// means "start from the very beginning," which is trivially consistent
// with any log. Once the check passes, entries are appended starting
// right after PrevLogIndex — any existing entry there is a stale/
// diverged leftover and gets overwritten along with everything after it
// (§5.3's "delete the existing entry and all that follow it"). Day 8's
// replicateToPeer is the caller, sending exactly the entries a peer is
// missing per the leader's nextIndex bookkeeping, and backing nextIndex
// off by one whenever this check rejects — that backoff-and-retry path
// only became reachable for real once this check existed to produce a
// genuine rejection.
//
// Day 6 adds one more rejection case: PrevLogIndex can fall BEFORE
// lastIncludedIndex, meaning this node has already compacted away the
// entry the leader wants to check against. Rejecting is conservative —
// it never risks accepting something unverified — but it's also a dead
// end on its own: the leader will keep backing nextIndex off and keep
// landing here again, since the entry it's checking against is gone for
// good, not just temporarily missing. Day 7's InstallSnapshot RPC is
// the leader-side fix — recognizing this exact situation and sending
// the whole snapshot instead of retrying AppendEntries forever.
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

	lastLogIndex, _ := r.lastLogInfoLocked()
	if args.PrevLogIndex > 0 {
		prevTerm, ok := r.termAtLocked(args.PrevLogIndex)
		if args.PrevLogIndex > lastLogIndex || !ok || prevTerm != args.PrevLogTerm {
			reply.Success = false
			// Still a legitimate leader for this term — just missing an
			// entry this node needs first. Reset the timer so this
			// rejection doesn't also trigger a spurious election; the
			// leader will retry with a lower PrevLogIndex.
			r.ResetElectionTimer()
			return nil
		}
	}

	if len(args.Entries) > 0 {
		r.log = append(r.log[:r.physicalIndexLocked(args.PrevLogIndex)+1], args.Entries...)
		r.persistLocked()
	}

	// Day 9: adopt the leader's commit progress. Capped at this node's own
	// last log index — never trust LeaderCommit past what was actually
	// just appended locally, since this node can't apply an entry it
	// doesn't have yet (Raft paper §5.3).
	if args.LeaderCommit > r.commitIndex {
		lastNewIndex, _ := r.lastLogInfoLocked()
		if args.LeaderCommit < lastNewIndex {
			r.commitIndex = args.LeaderCommit
		} else {
			r.commitIndex = lastNewIndex
		}
	}

	reply.Success = true
	r.ResetElectionTimer()
	return nil
}

// InstallSnapshot handles an incoming snapshot from the leader (Raft
// paper §7) — Day 7's answer to the gap Day 6's replicateToPeer left
// open: a follower whose nextIndex has fallen at or below the leader's
// lastIncludedIndex needs entries the leader has already compacted
// away and can never get via AppendEntries. Rather than rejecting that
// follower forever, the leader sends its entire compacted state in one
// RPC; this node discards its own log through LastIncludedIndex and
// adopts the leader's snapshot as its own.
//
// Two cases for what happens to THIS node's log, matching the paper's
// receiver implementation (Figure 13, steps 6-8):
//   - If this node's own log already has an entry at LastIncludedIndex
//     agreeing on term, everything after it is still valid — keep that
//     suffix rather than discarding real (and possibly not-yet-visible-
//     to-the-leader) entries the snapshot doesn't know about.
//   - Otherwise (this node's log is shorter, or diverges at that point)
//     there's nothing salvageable: discard the log entirely and rebuild
//     state purely from the snapshot.
//
// The state machine is notified via the same ApplyCh a normal committed
// entry would use, distinguished by SnapshotValid — from the state
// machine's point of view, "here's a snapshot to adopt wholesale"
// and "here's the next entry to apply" are just two different messages
// on the same door (see apply.go's ApplyMsg).
func (r *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	r.mu.Lock()

	if args.Term < r.currentTerm {
		reply.Term = r.currentTerm
		r.mu.Unlock()
		return nil
	}

	r.becomeFollowerLocked(args.Term)
	reply.Term = r.currentTerm
	r.ResetElectionTimer()

	if args.LastIncludedIndex <= r.lastIncludedIndex {
		// Stale or duplicate: this node already has at least this much
		// compacted (e.g. a retried RPC, or a race with a newer
		// snapshot this node installed some other way). Nothing to
		// install, and nothing new to hand the state machine either.
		r.mu.Unlock()
		return nil
	}

	if entryTerm, ok := r.termAtLocked(args.LastIncludedIndex); ok && entryTerm == args.LastIncludedTerm {
		r.log = append([]LogEntry(nil), r.log[r.physicalIndexLocked(args.LastIncludedIndex)+1:]...)
	} else {
		r.log = nil
	}
	r.lastIncludedIndex = args.LastIncludedIndex
	r.lastIncludedTerm = args.LastIncludedTerm
	r.snapshotData = append([]byte(nil), args.Data...)
	if r.commitIndex < args.LastIncludedIndex {
		r.commitIndex = args.LastIncludedIndex
	}
	if r.lastApplied < args.LastIncludedIndex {
		r.lastApplied = args.LastIncludedIndex
	}

	if r.persister != nil {
		if err := r.persister.SaveStateAndSnapshot(r.encodeStateLocked(), args.Data); err != nil {
			panic(fmt.Sprintf("raft: failed to persist installed snapshot: %v", err))
		}
	}

	// Queued, not sent directly — see pendingSnapshot's doc comment for
	// why this can't just send on r.ApplyCh from here. A newer
	// InstallSnapshot overwriting an older not-yet-delivered one here is
	// fine (and correct, not just harmless): the state machine only
	// ever needs the LATEST snapshot, since adopting one means
	// "discard whatever you have and replace it wholesale" — there's no
	// value in delivering an intermediate one first.
	r.pendingSnapshot = &ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	r.mu.Unlock()
	return nil
}
