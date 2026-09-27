// Package shardkv splits the keyspace of Stage 2's replicated KV store across
// several independent Raft groups. See 05-sharded-kv/README.md for concept
// notes and 05-sharded-kv/TASKS.md for the day-by-day build plan this file
// follows.
package shardkv

import "hash/fnv"

// NShards is the fixed number of shards the keyspace is divided into. It never
// changes: what changes over this stage is which GROUP owns each shard, never
// how many shards there are or which shard a key belongs to. That separation
// is what makes rebalancing possible at all — moving a shard moves a whole,
// well-defined slice of the keyspace, rather than forcing every key to be
// rehashed.
const NShards = 10

// Key2Shard maps a key to its shard. It must be a pure function of the key,
// identical for every client and every server forever, because a key's shard
// decides which group holds its data: two clients that disagreed about it
// would each write to a different group and never see each other's writes.
//
// FNV-1a rather than, say, the key's first byte: keys in real workloads share
// prefixes ("user:", "session:"), and a first-byte hash would send all of them
// to one shard. A hash mixes every byte, so prefixed keys still spread.
func Key2Shard(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % NShards)
}
