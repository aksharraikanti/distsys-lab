package raft

import "time"

// RunHeartbeats periodically drives replication whenever this node is
// Leader. It blocks, so the caller runs it in its own goroutine:
// `go node.RunHeartbeats()`. It shares stop signaling with
// RunElectionTimer — a single StopElectionTimer() call stops both loops,
// since they're really one node's "am I still alive/in charge" lifecycle.
//
// The name predates Day 8: each tick calls replicate() (log_replication.go),
// which sends every peer whatever log entries it's missing — an
// up-to-date peer just gets an empty Entries slice, which is exactly what
// a Day 5 heartbeat always was. There's no separate "heartbeat" RPC or
// path; a heartbeat is simply what replication looks like when there's
// nothing new to send.
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
				r.replicate()
			}
		}
	}
}
