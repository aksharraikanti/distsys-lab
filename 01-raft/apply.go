package raft

import "time"

// ApplyMsg is what RunApplyLoop delivers on ApplyCh once a log entry has
// been safely committed — the Raft paper's "apply to the state machine"
// step. This package only carries the entry to the door; it has no
// opinion about what's on the other side. Stage 2's fault-tolerant KV
// store is the natural first consumer.
type ApplyMsg struct {
	Index   int
	Term    int
	Command interface{}
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
			r.applyPending()
		}
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
		if nextApplied > len(r.log) {
			// Defensive, should never trigger: commitIndex is only ever
			// set to a value <= len(r.log) (advanceCommitIndexLocked and
			// the follower-side LeaderCommit cap both guarantee this).
			// But Day 10's log consistency check — the thing that
			// actually guarantees a committed entry can never be
			// truncated out from under it — doesn't exist yet, so this
			// guard exists to fail safe (skip, don't crash) instead of
			// panicking on an out-of-range index if that invariant is
			// ever violated before Day 10 lands.
			r.mu.Unlock()
			return
		}
		r.lastApplied = nextApplied
		entry := r.log[r.lastApplied-1] // 1-indexed
		index := r.lastApplied
		r.mu.Unlock()

		select {
		case r.ApplyCh <- ApplyMsg{Index: index, Term: entry.Term, Command: entry.Command}:
		case <-r.stopCh:
			return
		}
	}
}
