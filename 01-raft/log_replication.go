package raft

import "sync"

// replicate sends one round of AppendEntries to every peer, concurrently.
// Called periodically by RunHeartbeats whenever this node is Leader. Each
// peer gets whatever it's missing (from nextIndex[peer] onward) — a
// caught-up peer gets an empty Entries slice, which is what makes this
// the same call Day 5 wired up as a plain heartbeat, just no longer
// hardcoded to always be empty.
func (r *Raft) replicate() {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}
	peers := append([]int(nil), r.peers...)
	r.mu.Unlock()

	var wg sync.WaitGroup
	for _, peer := range peers {
		peer := peer
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.replicateToPeer(peer)
		}()
	}
	wg.Wait()
}

// replicateToPeer sends one AppendEntries RPC to peer, carrying whatever
// log entries it's missing per nextIndex[peer], and updates
// nextIndex/matchIndex from the result:
//   - success: matchIndex[peer] advances to the last index just sent,
//     and nextIndex[peer] moves to right after it.
//   - failure (the follower rejected because its log doesn't match at
//     PrevLogIndex): nextIndex[peer] backs off by one, so the next round
//     retries one entry further back. This is the simplest correct
//     recovery, not the fastest one — a real implementation often has
//     the follower report a hint to skip back multiple entries at once,
//     a reasonable stretch goal rather than something to build now.
//
// Day 10's log consistency check is what will make AppendEntries reject
// on an actual log mismatch; until then, a term-valid call always
// succeeds, so the failure branch here is unreachable through the live
// system — same situation Day 7's append logic was in before this day
// wired a real caller up to it.
//
// Day 6 added one more thing replicateToPeer has to know about: a peer
// whose nextIndex has fallen at or below lastIncludedIndex needs
// entries this node no longer has — they were compacted into a
// snapshot. Day 7 closes that gap for real: such a peer gets
// sendInstallSnapshot instead of an AppendEntries this node could never
// even construct a valid PrevLogIndex for.
func (r *Raft) replicateToPeer(peer int) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}
	next := r.nextIndex[peer]
	if next <= r.lastIncludedIndex {
		r.mu.Unlock()
		r.sendInstallSnapshot(peer)
		return
	}
	term := r.currentTerm
	leaderID := r.id
	prevLogIndex := next - 1
	prevLogTerm := 0
	if prevLogIndex >= 1 {
		// ok is guaranteed true: prevLogIndex == next-1 >= lastIncludedIndex
		// follows directly from the next <= lastIncludedIndex guard above.
		prevLogTerm, _ = r.termAtLocked(prevLogIndex)
	}
	lastLogIndex := r.lastIncludedIndex + len(r.log)
	var entries []LogEntry
	if next <= lastLogIndex {
		entries = append([]LogEntry(nil), r.log[r.physicalIndexLocked(next):]...)
	}
	leaderCommit := r.commitIndex
	r.mu.Unlock()

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     leaderID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}

	var reply AppendEntriesReply
	if err := r.transport.CallAppendEntries(peer, args, &reply); err != nil {
		return // unreachable peer — the next tick will retry
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if reply.Term > r.currentTerm {
		r.becomeFollowerLocked(reply.Term)
		return
	}
	if r.state != Leader || r.currentTerm != term {
		return // no longer leader of the term this reply belongs to
	}

	if reply.Success {
		newMatch := prevLogIndex + len(entries)
		if newMatch > r.matchIndex[peer] {
			r.matchIndex[peer] = newMatch
		}
		r.nextIndex[peer] = newMatch + 1
		r.advanceCommitIndexLocked()
		return
	}

	if r.nextIndex[peer] > 1 {
		r.nextIndex[peer]--
	}
}

// sendInstallSnapshot sends this node's current snapshot to peer, for
// the one case AppendEntries can never resolve on its own: peer's
// nextIndex has fallen at or below lastIncludedIndex, meaning the
// entries it needs no longer exist in this node's log. Reads
// snapshotData directly (the in-memory copy every node keeps regardless
// of persister — see its doc comment on the Raft struct) rather than
// the persister, since a persister-less leader still needs to be able
// to serve this.
func (r *Raft) sendInstallSnapshot(peer int) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}
	term := r.currentTerm
	leaderID := r.id
	lastIncludedIndex := r.lastIncludedIndex
	lastIncludedTerm := r.lastIncludedTerm
	data := append([]byte(nil), r.snapshotData...)
	r.mu.Unlock()

	args := &InstallSnapshotArgs{
		Term:              term,
		LeaderID:          leaderID,
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Data:              data,
	}
	var reply InstallSnapshotReply
	if err := r.transport.CallInstallSnapshot(peer, args, &reply); err != nil {
		return // unreachable peer — the next tick will retry
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if reply.Term > r.currentTerm {
		r.becomeFollowerLocked(reply.Term)
		return
	}
	if r.state != Leader || r.currentTerm != term {
		return // no longer leader of the term this reply belongs to
	}

	// The follower now has everything through lastIncludedIndex —
	// resume ordinary AppendEntries replication right after it,
	// exactly as a successful AppendEntries reply would advance these.
	if lastIncludedIndex > r.matchIndex[peer] {
		r.matchIndex[peer] = lastIncludedIndex
	}
	if lastIncludedIndex+1 > r.nextIndex[peer] {
		r.nextIndex[peer] = lastIncludedIndex + 1
	}
	r.advanceCommitIndexLocked()
}
