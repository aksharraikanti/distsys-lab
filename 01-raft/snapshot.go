package raft

import "fmt"

// physicalIndexLocked converts an absolute, paper-style (1-indexed) log
// index into the position that index occupies in r.log — the in-memory
// slice that, after any snapshot, only holds entries AFTER
// lastIncludedIndex. Only meaningful for an index strictly greater than
// lastIncludedIndex; callers must check that first (termAtLocked does,
// for the one case — index == lastIncludedIndex — that's meaningful but
// NOT indexable this way).
func (r *Raft) physicalIndexLocked(index int) int {
	return index - r.lastIncludedIndex - 1
}

// termAtLocked returns the term of the entry at absolute index index,
// and whether that term is actually knowable from what this node
// currently holds. Three cases:
//   - index == lastIncludedIndex: known without consulting r.log at all
//     — it's exactly the entry the most recent snapshot replaced.
//   - index > lastIncludedIndex and still within the log: read directly
//     out of r.log via physicalIndexLocked.
//   - anything else (index < lastIncludedIndex, i.e. compacted away
//     entirely, or index beyond the log's current end): not known here.
//     Every caller treats !ok the same way a real Raft node has to:
//     as "can't verify this the normal way" — AppendEntries rejects,
//     and (once Day 7 exists) that rejection is what eventually routes
//     the caller to InstallSnapshot instead.
func (r *Raft) termAtLocked(index int) (term int, ok bool) {
	if index == r.lastIncludedIndex {
		return r.lastIncludedTerm, true
	}
	if index <= r.lastIncludedIndex {
		return 0, false
	}
	physIdx := r.physicalIndexLocked(index)
	if physIdx < 0 || physIdx >= len(r.log) {
		return 0, false
	}
	return r.log[physIdx].Term, true
}

// RaftStateSize reports how many bytes currentTerm/votedFor/log would
// take up if persisted right now — the exact quantity a size-based
// snapshot policy needs to watch, since the log is the only one of the
// three that grows without bound absent compaction. This is computed
// fresh from the encoder, not read back from the persister, so it's
// meaningful even for a node with no persister attached at all (most
// tests): "how big is my log getting," not "how much have I actually
// written to disk."
func (r *Raft) RaftStateSize() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.encodeStateLocked())
}

// Snapshot tells this node that its state machine has already applied
// every entry through index (inclusive) and has serialized its own
// state as data — so every log entry through index is now redundant,
// fully recoverable from data instead, and safe to discard. The state
// machine (KVServer, in Stage 2) owns the WHEN — typically "RaftStateSize
// crossed some threshold" — and HOW to serialize its own data; Raft's
// only job here is the compaction itself: trim r.log, remember what the
// discarded prefix's last entry was (lastIncludedIndex/Term, so
// AppendEntries and elections can still reason about it without the
// entry itself), and persist the result.
//
// A stale call (index already at or below lastIncludedIndex — e.g. a
// slow caller racing a newer compaction, or, once Day 7 exists, a
// just-installed snapshot from a leader) is not an error: it's a no-op,
// since this node has already compacted at least that far. An index
// past this node's own log is a genuine misuse — the caller claims to
// have applied something this node never even logged — and is reported
// as an error rather than silently ignored or clamped.
func (r *Raft) Snapshot(index int, data []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if index <= r.lastIncludedIndex {
		return nil
	}
	lastLogIndex, _ := r.lastLogInfoLocked()
	if index > lastLogIndex {
		return fmt.Errorf("raft: cannot snapshot through index %d, past this node's last log index %d", index, lastLogIndex)
	}

	// termAtLocked/physicalIndexLocked both read lastIncludedIndex, so
	// this MUST run before that field is overwritten below.
	term, _ := r.termAtLocked(index) // ok is guaranteed true by the two bounds checks above
	physIdx := r.physicalIndexLocked(index)
	r.log = append([]LogEntry(nil), r.log[physIdx+1:]...)
	r.lastIncludedIndex = index
	r.lastIncludedTerm = term

	if r.persister != nil {
		if err := r.persister.SaveStateAndSnapshot(r.encodeStateLocked(), data); err != nil {
			panic(fmt.Sprintf("raft: failed to persist snapshot: %v", err))
		}
	}
	return nil
}

// ReadSnapshot returns whatever snapshot bytes this node currently has
// persisted, or nil if it has none (no persister attached, or a
// persister that's never had one saved). A state machine calls this
// once, at construction, to recover state a plain ApplyCh replay can no
// longer provide on its own: once Snapshot has trimmed the log, the
// entries through lastIncludedIndex are gone for good — the ONLY
// remaining record of what they did is this snapshot, so a state
// machine that skips this step silently starts from an incomplete
// state after any restart that follows a snapshot.
func (r *Raft) ReadSnapshot() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.persister == nil {
		return nil
	}
	data, err := r.persister.ReadSnapshot()
	if err != nil {
		panic(fmt.Sprintf("raft: failed to read persisted snapshot: %v", err))
	}
	return data
}
