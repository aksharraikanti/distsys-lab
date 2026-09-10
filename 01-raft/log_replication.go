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
func (r *Raft) replicateToPeer(peer int) {
	r.mu.Lock()
	if r.state != Leader {
		r.mu.Unlock()
		return
	}
	term := r.currentTerm
	leaderID := r.id
	next := r.nextIndex[peer]
	prevLogIndex := next - 1
	prevLogTerm := 0
	if prevLogIndex >= 1 && prevLogIndex <= len(r.log) {
		prevLogTerm = r.log[prevLogIndex-1].Term
	}
	var entries []LogEntry
	if next <= len(r.log) {
		entries = append([]LogEntry(nil), r.log[next-1:]...)
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
