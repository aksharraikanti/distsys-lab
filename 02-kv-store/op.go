// Package kvstore is a fault-tolerant key-value store built on top of
// Stage 1's from-scratch Raft implementation. See 02-kv-store/README.md
// for concept notes and 02-kv-store/TASKS.md for the day-by-day build
// plan this file follows.
package kvstore

import "encoding/gob"

// Op is the state-machine command every KV mutation is logged as — the
// concrete type that flows through raft.LogEntry.Command (an
// interface{}). Get is deliberately not represented here: it doesn't
// mutate state, so Day 1 has no reason to log it (whether reads should
// also go through Raft, for linearizability, is a later day's decision).
//
// ClientID/SeqNum (Day 3) identify which logical client request this Op
// came from. A client that can't tell whether its last request actually
// succeeded (a timeout, a leader change) retries by sending the SAME
// ClientID+SeqNum again — the retry gets its own new log index (Raft has
// no idea it's a retry), but the state machine recognizes the SeqNum has
// already been applied and skips re-applying its effect. SeqNum must
// start at 1 and increase strictly per new logical request from a given
// client; 0 is never a valid in-use value, since it's Go's zero value and
// treating it as valid would make every never-set Op collide with
// "already applied."
type Op struct {
	Type  string // "Put" or "Append"
	Key   string
	Value string

	ClientID int64
	SeqNum   int64
}

func init() {
	// encoding/gob can't decode a concrete type stored in an interface{}
	// field (raft.LogEntry.Command, here) unless that type was
	// registered first — Stage 1's 01-raft/persist.go left this exact
	// note for whichever concrete Command type Stage 2 introduced. This
	// is that registration.
	gob.Register(Op{})
}
