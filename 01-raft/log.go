package raft

// Propose appends a new entry for command to this node's own log, if it
// is currently the Leader. It returns the index and term the entry was
// assigned, and true — or (0, 0, false) if this node isn't the leader, so
// the caller knows to retry against whichever node actually is.
//
// This only appends to the LEADER'S own log. It does not replicate the
// entry to followers or wait for it to commit — sending entries out
// (matchIndex/nextIndex, retries) is Day 8's job, and knowing when an
// entry is safely committed is Day 9's. Propose is the entry point that
// those later days build on: "a client asked for this to happen" has to
// land somewhere before it can go anywhere else.
func (r *Raft) Propose(command interface{}) (index int, term int, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != Leader {
		return 0, 0, false
	}

	r.log = append(r.log, LogEntry{Term: r.currentTerm, Command: command})
	return len(r.log), r.currentTerm, true // 1-indexed, matching lastLogInfoLocked
}
