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
type Op struct {
	Type  string // "Put" or "Append"
	Key   string
	Value string
}

func init() {
	// encoding/gob can't decode a concrete type stored in an interface{}
	// field (raft.LogEntry.Command, here) unless that type was
	// registered first — Stage 1's 01-raft/persist.go left this exact
	// note for whichever concrete Command type Stage 2 introduced. This
	// is that registration.
	gob.Register(Op{})
}
