package shardkv

import "encoding/gob"

// ctrlerOp is the state-machine command that flows through the shard
// controller's Raft log — the config-history analogue of 02-kv-store's Op.
// Query has no entry here: like Get, a Query doesn't change the config
// history, so there's no reason to log it, and every replica can answer it
// straight from its own local configs slice.
//
// A single struct carries all three mutating operations' arguments rather
// than three separate command types, mirroring Op's own "Type string plus
// whichever fields that type uses" shape — simpler than teaching applyLoop
// to type-switch across multiple concrete command types for what's still
// fundamentally one log of "here's the next config version."
type ctrlerOp struct {
	Type string // "Join", "Leave", "Move", or "Noop"

	JoinGroups map[int][]string // Join
	LeaveGIDs  []int            // Leave
	MoveShard  int              // Move
	MoveGID    int              // Move

	// ClientID/SeqNum are the exact dedup contract 02-kv-store's Op uses —
	// see its doc comment. A retried Join/Leave/Move (same ClientID+SeqNum
	// landing at a new log index) must apply its effect at most once, or a
	// client that can't tell whether its first attempt actually committed
	// would risk double-joining a group or double-moving a shard on retry.
	ClientID int64
	SeqNum   int64
}

func init() {
	// Same reason as 02-kv-store's own registration: encoding/gob can't
	// decode a concrete type stored in an interface{} field
	// (raft.LogEntry.Command) unless it's registered first.
	gob.Register(ctrlerOp{})
}
