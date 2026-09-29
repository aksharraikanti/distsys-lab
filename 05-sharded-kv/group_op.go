package shardkv

import "encoding/gob"

// groupOp is the state-machine command that flows through one shard GROUP's
// own Raft log — 02-kv-store's Op, extended with two kinds of entry Stage 2
// never needed: a config change, and a migration landing. Put/Append/Noop
// mean exactly what they do in kvstore.Op. Config carries a whole new
// Config, adopted only if it is EXACTLY one version past whatever this
// replica has already applied (see GroupServer.applyLoop) — the mechanism
// that makes "does my group own this shard" a question answered from
// REPLICATED state, not a locally polled variable that could disagree
// between replicas or lag behind a leader's own election. Migrate lands one
// shard's pulled data and its slice of the donor's dedup table — see
// GroupServer.migrationLoop and applyLoop's own doc comments for why the
// dedup table has to travel WITH the shard, not stay behind.
//
// Every kind of entry shares one log, in one order, which is the whole
// point: whichever one commits first is authoritative for every entry that
// comes after it, on every replica, including which shard a Put/Append is
// checked against, and whether a shard is even ready to serve yet, at apply
// time.
type groupOp struct {
	Type string // "Put", "Append", "Config", "Migrate", or "Noop"

	Key   string // Put, Append
	Value string // Put, Append

	Config Config // Config

	Shard    int               // Migrate
	Data     map[string]string // Migrate: this shard's key/value pairs
	DupTable map[int64]int64   // Migrate: the donor's WHOLE dedup table as of the pull

	ClientID int64
	SeqNum   int64
}

func init() {
	gob.Register(groupOp{})
}
