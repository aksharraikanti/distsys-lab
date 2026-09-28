package shardkv

import "encoding/gob"

// groupOp is the state-machine command that flows through one shard GROUP's
// own Raft log — 02-kv-store's Op, extended with a third kind of entry this
// stage needs that Stage 2 never did: a config change. Put/Append/Noop mean
// exactly what they do in kvstore.Op. Config carries a whole new Config,
// adopted only if it is EXACTLY one version past whatever this replica has
// already applied (see GroupServer.applyLoop) — the mechanism that makes
// "does my group own this shard" a question answered from REPLICATED state,
// not a locally polled variable that could disagree between replicas or
// lag behind a leader's own election.
//
// Put/Append and Config entries share one log, in one order, which is the
// whole point: whichever one commits first is authoritative for every entry
// that comes after it, on every replica, including which shard a
// Put/Append is checked against at apply time.
type groupOp struct {
	Type string // "Put", "Append", "Config", or "Noop"

	Key   string // Put, Append
	Value string // Put, Append

	Config Config // Config

	ClientID int64
	SeqNum   int64
}

func init() {
	gob.Register(groupOp{})
}
