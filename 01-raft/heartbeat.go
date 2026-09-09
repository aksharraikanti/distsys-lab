package raft

import (
	"sync"
	"time"
)

// RunHeartbeats sends periodic empty AppendEntries (heartbeats) to every
// peer whenever this node is Leader. It blocks, so the caller runs it in
// its own goroutine: `go node.RunHeartbeats()`. It shares stop signaling
// with RunElectionTimer — a single StopElectionTimer() call stops both
// loops, since they're really one node's "am I still alive/in charge"
// lifecycle.
//
// A node that isn't currently Leader still runs this loop (so it's ready
// the instant it wins an election) — each tick is simply a no-op until
// then, checked via State() rather than a separate started/stopped flag.
func (r *Raft) RunHeartbeats() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			if r.State() == Leader {
				r.sendHeartbeats()
			}
		}
	}
}

// sendHeartbeats fires one round of empty AppendEntries at every peer,
// concurrently. If any reply reveals a higher term, this node's
// leadership is stale — mirroring startElection's identical guard, it
// steps down to Follower and adopts that term instead of continuing to
// act as leader on a term that's already over.
func (r *Raft) sendHeartbeats() {
	r.mu.Lock()
	term := r.currentTerm
	leaderID := r.id
	peers := append([]int(nil), r.peers...)
	r.mu.Unlock()

	args := &AppendEntriesArgs{Term: term, LeaderID: leaderID}

	var wg sync.WaitGroup
	for _, peer := range peers {
		peer := peer
		wg.Add(1)
		go func() {
			defer wg.Done()

			var reply AppendEntriesReply
			if err := r.transport.CallAppendEntries(peer, args, &reply); err != nil {
				return // unreachable peer — nothing to do until the next tick
			}

			r.mu.Lock()
			defer r.mu.Unlock()
			if reply.Term > r.currentTerm {
				r.becomeFollowerLocked(reply.Term)
			}
		}()
	}
	wg.Wait()
}
