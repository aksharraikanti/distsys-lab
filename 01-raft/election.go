package raft

import "sync"

// lastLogInfoLocked returns the index and term of the node's last log
// entry, or (lastIncludedIndex, lastIncludedTerm) if r.log is empty —
// "nothing here yet" before Day 6 ever existed (0, 0, the Raft paper's
// own sentinel for an empty log), and after Day 6 it means "everything
// through lastIncludedIndex was compacted into a snapshot, and nothing
// has been appended since" — lastIncludedIndex/Term describe the entry
// that snapshot replaced, so they're exactly the right stand-in index/
// term. Entries are 1-indexed (absolute index 0 is the sentinel), and
// r.log only holds entries AFTER lastIncludedIndex, so the last entry's
// absolute index is lastIncludedIndex + the physical log's length.
func (r *Raft) lastLogInfoLocked() (index, term int) {
	if len(r.log) == 0 {
		return r.lastIncludedIndex, r.lastIncludedTerm
	}
	last := r.log[len(r.log)-1]
	return r.lastIncludedIndex + len(r.log), last.Term
}

// candidateLogIsUpToDateLocked implements the Raft paper's §5.4.1
// up-to-date rule: a candidate's log is at least as up-to-date as this
// node's if the candidate's last entry has a later term, or — when the
// terms match — if the candidate's log is at least as long. A node must
// never vote for a candidate whose log could lose committed entries.
func (r *Raft) candidateLogIsUpToDateLocked(candidateLastIndex, candidateLastTerm int) bool {
	myLastIndex, myLastTerm := r.lastLogInfoLocked()
	if candidateLastTerm != myLastTerm {
		return candidateLastTerm > myLastTerm
	}
	return candidateLastIndex >= myLastIndex
}

// startElection sends RequestVote RPCs to every peer concurrently and, if
// a majority of the cluster (including the candidate's own vote) grants
// it before the election is preempted, transitions to Leader. Called by
// RunElectionTimer whenever BecomeCandidate succeeds — always in its own
// goroutine, so a slow or unreachable peer can never block the election
// timer loop itself.
//
// "Preempted" covers two cases the response handler below has to guard
// against: a peer's reply carries a higher term (this node steps down
// instead of finishing the count), or the node's own state has already
// moved on by the time a reply arrives (it already won, already lost to a
// competing election, or it's already run a newer election) — replies
// belonging to a stale term must never be allowed to swing a later one.
func (r *Raft) startElection() {
	r.mu.Lock()
	term := r.currentTerm
	candidateID := r.id
	peers := append([]int(nil), r.peers...)
	lastLogIndex, lastLogTerm := r.lastLogInfoLocked()
	r.mu.Unlock()

	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  candidateID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	votes := 1 // the candidate votes for itself (see becomeCandidateLocked)
	total := len(peers) + 1
	majority := total/2 + 1

	var wg sync.WaitGroup
	for _, peer := range peers {
		peer := peer
		wg.Add(1)
		go func() {
			defer wg.Done()

			var reply RequestVoteReply
			if err := r.transport.CallRequestVote(peer, args, &reply); err != nil {
				return // unreachable peer counts as "no response," not a vote
			}

			r.mu.Lock()
			defer r.mu.Unlock()

			if reply.Term > r.currentTerm {
				r.becomeFollowerLocked(reply.Term)
				return
			}
			if r.state != Candidate || r.currentTerm != term {
				return // this election has already been decided or superseded
			}
			if !reply.VoteGranted {
				return
			}

			votes++
			if votes >= majority {
				_ = r.becomeLeaderLocked()
			}
		}()
	}
	wg.Wait()
}
