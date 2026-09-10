package raft

import "sort"

// CommitIndex returns the highest log index known to be committed. Safe
// to call concurrently.
func (r *Raft) CommitIndex() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.commitIndex
}

// advanceCommitIndexLocked implements the Raft paper's commit rule
// (§5.3, §5.4.2): if there exists an index N > commitIndex such that a
// majority of the cluster (leader included) has matchIndex >= N, AND
// log[N] belongs to the LEADER'S CURRENT TERM, then N is safe to commit.
//
// The current-term restriction is the paper's Figure 8 safety fix. A
// majority holding an entry from an OLDER term is not by itself enough —
// that entry could still be overwritten by a future leader who never saw
// it, if no later entry has anchored it into a term that leader agrees
// on. Once a later entry FROM THE CURRENT LEADER'S TERM reaches a
// majority, though, every entry before it becomes safe too (any future
// leader who could win an election must already have replicated at
// least that current-term entry, by §5.4.1's up-to-date voting rule).
// Committing indirectly like this — never directly committing an
// older-term entry on its own majority — is what closes the gap Figure 8
// describes.
//
// Called from wherever matchIndex[peer] changes (replicateToPeer) and
// from Propose (so a single-node cluster, with no peers to reply at all,
// still commits its own entries immediately — the leader's own log
// always trivially satisfies a majority of one).
func (r *Raft) advanceCommitIndexLocked() {
	if r.state != Leader {
		return
	}

	lastIndex, _ := r.lastLogInfoLocked()
	matches := make([]int, 0, len(r.peers)+1)
	matches = append(matches, lastIndex) // the leader always has all of its own log
	for _, peer := range r.peers {
		matches = append(matches, r.matchIndex[peer])
	}
	sort.Sort(sort.Reverse(sort.IntSlice(matches)))

	majority := len(matches)/2 + 1
	// After sorting descending, matches[majority-1] is the largest N for
	// which at least `majority` servers have matchIndex >= N.
	candidate := matches[majority-1]

	if candidate > r.commitIndex && candidate >= 1 && candidate <= len(r.log) && r.log[candidate-1].Term == r.currentTerm {
		r.commitIndex = candidate
	}
}
