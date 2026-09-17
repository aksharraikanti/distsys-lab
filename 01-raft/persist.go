package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// persistedState is exactly the Raft paper's "persistent state on all
// servers" (Figure 2) — currentTerm, votedFor, and log — plus, since
// Day 6, lastIncludedIndex/lastIncludedTerm: the paper doesn't name
// these separately because it treats the log as never being compacted,
// but once Snapshot has trimmed a prefix off r.log, that prefix's last
// entry has to be remembered right alongside the log itself, or a
// restart would have no way to know what index/term the (now-shorter)
// log actually starts after. Nothing else (state, commitIndex,
// lastApplied, nextIndex/matchIndex) is persisted: they're all either
// re-derivable or paper-mandated to reset on restart (a restarted node
// always comes back as a Follower, regardless of what it was before it
// crashed) — see restoreLocked for the one place lastApplied/commitIndex
// DO need an explicit bump on restore, despite not being persisted
// themselves.
type persistedState struct {
	CurrentTerm       int
	VotedFor          int
	Log               []LogEntry
	LastIncludedIndex int
	LastIncludedTerm  int
}

func init() {
	// encoding/gob can't encode/decode a concrete value stored in an
	// interface{} field (LogEntry.Command here) unless that concrete
	// type has been registered first — an easy first-timer trap, since
	// the failure only shows up at decode time, as a "gob: name not
	// registered for interface" error, not at compile time. This covers
	// every test in this stage (all Command values are strings). A
	// future consumer using a different concrete Command type — Stage
	// 2's KV store, for instance — needs its own gob.Register call for
	// that type before persistence will round-trip it correctly.
	gob.Register("")
}

// encodeStateLocked serializes currentTerm, votedFor, and log for
// SaveState. Encoding our own in-memory state should never fail short of
// a programmer error (e.g. a Command type that was never gob.Register'd)
// — panicking here rather than silently swallowing the error is
// deliberate: a state that claims to be persisted but wasn't is exactly
// the class of bug this whole day exists to close.
func (r *Raft) encodeStateLocked() []byte {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(persistedState{
		CurrentTerm:       r.currentTerm,
		VotedFor:          r.votedFor,
		Log:               r.log,
		LastIncludedIndex: r.lastIncludedIndex,
		LastIncludedTerm:  r.lastIncludedTerm,
	}); err != nil {
		panic(fmt.Sprintf("raft: failed to encode persistent state: %v", err))
	}
	return buf.Bytes()
}

// persistLocked writes currentTerm/votedFor/log to r.persister. Called by
// every code path that mutates any of those three fields, before that
// mutation becomes visible outside this node. A nil persister (the
// default from NewRaft) makes this a no-op — most tests don't exercise
// restart behavior and would rather not pay for encoding on every
// mutation.
func (r *Raft) persistLocked() {
	if r.persister == nil {
		return
	}
	if err := r.persister.SaveState(r.encodeStateLocked()); err != nil {
		// A failed persist means this node can no longer safely claim to
		// have durably recorded its vote/term/log. Continuing as if
		// nothing happened would silently reintroduce the exact bug this
		// day exists to close — fail loud instead.
		panic(fmt.Sprintf("raft: failed to persist state: %v", err))
	}
}

// restoreLocked reads any previously-persisted state from r.persister and
// restores it. Called exactly once, from newRaft, before the node does
// anything else. A nil persister, or one that's never had SaveState
// called on it (a genuine first-ever boot), leaves the node at its
// normal fresh-start defaults.
func (r *Raft) restoreLocked() {
	if r.persister == nil {
		return
	}
	data, err := r.persister.ReadState()
	if err != nil {
		panic(fmt.Sprintf("raft: failed to read persisted state: %v", err))
	}
	if len(data) == 0 {
		return
	}
	var state persistedState
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&state); err != nil {
		panic(fmt.Sprintf("raft: failed to decode persisted state: %v", err))
	}
	r.currentTerm = state.CurrentTerm
	r.votedFor = state.VotedFor
	r.log = state.Log
	r.lastIncludedIndex = state.LastIncludedIndex
	r.lastIncludedTerm = state.LastIncludedTerm
	if r.lastIncludedIndex > 0 {
		// snapshotData isn't part of persistedState itself (it's a
		// separate, independently-stored blob — see Persister's doc
		// comment), so it has to be re-read from the persister
		// explicitly here. Without this, a restarted node would
		// correctly remember THAT it has a snapshot (lastIncludedIndex
		// > 0) while having lost the snapshot's actual bytes — fine for
		// nothing but a state machine restoring on this exact node's
		// own restart (Day 6), and silently wrong the moment this node
		// becomes leader and needs to InstallSnapshot a lagging
		// follower (Day 7).
		data, err := r.persister.ReadSnapshot()
		if err != nil {
			panic(fmt.Sprintf("raft: failed to read persisted snapshot: %v", err))
		}
		r.snapshotData = data
	}
	// commitIndex/lastApplied are volatile and normally start at their
	// zero value on every restart — re-derived by replaying the log from
	// the beginning. That stops being true once a snapshot has ever
	// trimmed a prefix off the log (Day 6): the entries through
	// lastIncludedIndex are gone for good, so "replay from the
	// beginning" can only ever start at lastIncludedIndex now, not 0.
	// Leaving these at 0 here would make applyPending try to apply
	// index 1 first, which no longer exists in r.log at all — an
	// out-of-range read, not just a stale one.
	if r.lastIncludedIndex > r.commitIndex {
		r.commitIndex = r.lastIncludedIndex
	}
	if r.lastIncludedIndex > r.lastApplied {
		r.lastApplied = r.lastIncludedIndex
	}
}
