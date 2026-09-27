package shardkv

import (
	"fmt"
	"sort"
)

// Config says which group owns each shard, and how to reach each group.
//
// Day 1 uses a Config that is fixed for a client's whole lifetime. Num (the
// version) is already here because Day 2's configurations are versioned —
// every change produces a new Config with a higher Num — but nothing reads it
// yet.
type Config struct {
	Num    int              // configuration version
	Shards [NShards]int     // Shards[s] is the id of the group that owns shard s
	Groups map[int][]string // group id -> the addresses of that group's servers
}

// NewStaticConfig assigns shards to the given groups round-robin, in ascending
// group-id order, so the result is deterministic: with ids [1 2 3] and 10
// shards, group 1 gets shards 0,3,6,9 and groups 2 and 3 get three each.
//
// This is deliberately the crudest possible assignment. Day 2 replaces it with
// a real rebalance whose defining property — moving as FEW shards as possible
// when membership changes — this one does not have: adding a group here would
// reshuffle nearly everything.
func NewStaticConfig(groups map[int][]string) Config {
	gids := make([]int, 0, len(groups))
	for gid := range groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids) // map iteration order is random; sort or the result is too
	cfg := Config{Num: 1, Groups: groups}
	if len(gids) == 0 {
		return cfg
	}
	for s := range cfg.Shards {
		cfg.Shards[s] = gids[s%len(gids)]
	}
	return cfg
}

// Validate checks that the config can actually route: at least one group, and
// every shard assigned to a group that has at least one address. A client
// built on an unroutable config would fail on some keys and not others, far
// from the mistake, so it is rejected up front.
func (c Config) Validate() error {
	if len(c.Groups) == 0 {
		return fmt.Errorf("shardkv: config has no groups")
	}
	for s, gid := range c.Shards {
		addrs, ok := c.Groups[gid]
		if !ok {
			return fmt.Errorf("shardkv: shard %d is assigned to group %d, which is not in Groups", s, gid)
		}
		if len(addrs) == 0 {
			return fmt.Errorf("shardkv: group %d (owner of shard %d) has no addresses", gid, s)
		}
	}
	return nil
}
