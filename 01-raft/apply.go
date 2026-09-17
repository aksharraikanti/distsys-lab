package raft

import "time"

// ApplyMsg is what RunApplyLoop (or, since Day 7, InstallSnapshot)
// delivers on ApplyCh — the Raft paper's "apply to the state machine"
// step, plus "adopt this snapshot instead." This package only carries
// the message to the door; it has no opinion about what's on the other
// side. Stage 2's fault-tolerant KV store is the natural first
// consumer.
//
// Exactly one of CommandValid/SnapshotValid is true on any given
// message — they're mutually exclusive views of the same channel, the
// same shape MIT 6.5840's own labs use: a normal committed entry
// (Index/Term/Command) needs applying one at a time, in order, the way
// applyPending always has; an installed snapshot (Day 7) needs the
// state machine to instead discard whatever it has and adopt Snapshot
// wholesale, since the individual entries it replaces were compacted
// away before this node ever saw them and will never arrive as
// CommandValid messages of their own.
type ApplyMsg struct {
	CommandValid bool
	Index        int
	Term         int
	Command      interface{}

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
}

// RunApplyLoop periodically checks whether commitIndex has advanced past
// lastApplied, and if so, delivers each newly committed entry — in order,
// exactly once — on ApplyCh. It blocks, so run it in its own goroutine:
// `go node.RunApplyLoop()`. Shares stop signaling with RunElectionTimer
// and RunHeartbeats via stopCh.
//
// This runs on every node, not just the leader — a follower's own
// commitIndex (kept in sync via AppendEntries' LeaderCommit) drives its
// own apply loop independently. That's what makes ApplyCh meaningful on
// any node: whichever one a state machine is attached to, it sees the
// same committed entries in the same order.
//
// Polling on a ticker rather than condvar-signaled, same reasoning as
// RunHeartbeats: an explicit, easy-to-reason-about mechanism over a
// sync.Cond's trickier wake-up-on-shutdown edge cases, and at this tick
// granularity the added latency is negligible.
func (r *Raft) RunApplyLoop() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			// Order matters: a snapshot installed since the last tick
			// (Day 7) must reach the state machine before any regular
			// entry that came after it, so it's delivered first, from
			// this same goroutine — see pendingSnapshot's doc comment
			// on the Raft struct for why nothing else may ever send on
			// ApplyCh.
			r.deliverPendingSnapshot()
			r.applyPending()
		}
	}
}

// deliverPendingSnapshot sends whatever snapshot InstallSnapshot (Day
// 7) queued up since the last tick, if any, to the state machine via
// ApplyCh — see pendingSnapshot's doc comment on the Raft struct for
// why this, not InstallSnapshot's own RPC-handler goroutine, is the
// only thing that may ever send on that channel.
func (r *Raft) deliverPendingSnapshot() {
	r.mu.Lock()
	msg := r.pendingSnapshot
	r.pendingSnapshot = nil
	r.mu.Unlock()
	if msg == nil {
		return
	}

	select {
	case r.ApplyCh <- *msg:
	case <-r.stopCh:
	}
}

// applyPending delivers every entry between lastApplied and commitIndex,
// one at a time. The lock is released before each channel send — ApplyCh
// can be arbitrarily slow to drain, and it must never be able to stall
// every other operation on this node while holding r.mu.
func (r *Raft) applyPending() {
	for {
		r.mu.Lock()
		if r.commitIndex <= r.lastApplied {
			r.mu.Unlock()
			return
		}
		nextApplied := r.lastApplied + 1
		lastLogIndex := r.lastIncludedIndex + len(r.log)
		if nextApplied > lastLogIndex {
			// Defensive, should never trigger: commitIndex is only ever
			// set to a value <= lastLogIndex (advanceCommitIndexLocked
			// and the follower-side LeaderCommit cap both guarantee
			// this). But Day 10's log consistency check — the thing that
			// actually guarantees a committed entry can never be
			// truncated out from under it — doesn't exist yet, so this
			// guard exists to fail safe (skip, don't crash) instead of
			// panicking on an out-of-range index if that invariant is
			// ever violated before Day 10 lands.
			r.mu.Unlock()
			return
		}
		r.lastApplied = nextApplied
		// nextApplied > lastIncludedIndex always holds here: Snapshot
		// (Day 6) only ever compacts through an index the state machine
		// already confirmed it applied, so lastApplied — and therefore
		// nextApplied — never falls behind lastIncludedIndex.
		entry := r.log[r.physicalIndexLocked(nextApplied)]
		index := r.lastApplied
		r.mu.Unlock()

		select {
		case r.ApplyCh <- ApplyMsg{CommandValid: true, Index: index, Term: entry.Term, Command: entry.Command}:
		case <-r.stopCh:
			return
		}
	}
}
