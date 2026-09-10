package raft

// Propose appends a new entry for command to this node's own log, if it
// is currently the Leader. It returns the index and term the entry was
// assigned, and true — or (0, 0, false) if this node isn't the leader, so
// the caller knows to retry against whichever node actually is.
//
// This appends to the LEADER'S own log and, since the leader always
// trivially satisfies a majority of one, immediately checks whether that
// alone is enough to commit — the only cluster shape where it is is a
// single-node cluster with no peers to reply at all. Everywhere else,
// advanceCommitIndexLocked here is a no-op that gets superseded once
// replicateToPeer's replies actually reach a majority. Propose does not
// itself replicate the entry to followers — that's replicateToPeer's job
// (Day 8), triggered by the next RunHeartbeats tick, not by Propose.
func (r *Raft) Propose(command interface{}) (index int, term int, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader {
		return 0, 0, false
	}

	r.log = append(r.log, LogEntry{Term: r.currentTerm, Command: command})
	r.advanceCommitIndexLocked()
	return len(r.log), r.currentTerm, true // 1-indexed, matching lastLogInfoLocked
}
