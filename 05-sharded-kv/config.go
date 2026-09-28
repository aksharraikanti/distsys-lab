package shardkv

import (
	"fmt"
	"sort"
)

// Config says which group owns each shard, and how to reach each group.
//
// Day 1 uses a Config that is fixed for a client's whole lifetime. Num (the
// version) is already here because Day 2's configurations are versioned —
// every change produces a new Config with a higher Num.
//
// Group id 0 is reserved to mean "no group": Leave can strip every owner from
// a shard, and the zero Config has to be a valid starting point for Join to
// build on. A real group id is always >= 1.
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

// Join returns a new config with newGroups added to cfg's groups and the
// shards rebalanced across the resulting set. It's a pure function of its
// inputs — no clock, no randomness, no map-iteration-order leakage — so every
// replica of a future controller (Day 3) that applies the same Join gets the
// identical Config back, byte for byte.
func Join(cfg Config, newGroups map[int][]string) Config {
	groups := make(map[int][]string, len(cfg.Groups)+len(newGroups))
	for gid, addrs := range cfg.Groups {
		groups[gid] = addrs
	}
	for gid, addrs := range newGroups {
		groups[gid] = addrs
	}
	return Config{
		Num:    cfg.Num + 1,
		Groups: groups,
		Shards: rebalance(cfg.Shards, groups),
	}
}

// Leave returns a new config with the given group ids removed and their
// shards redistributed across whatever groups remain. Removing every group
// leaves every shard owned by group 0 (see Config's doc comment) — a config
// Validate rejects, same as it rejects one with no groups at all.
func Leave(cfg Config, gids []int) Config {
	groups := make(map[int][]string, len(cfg.Groups))
	for gid, addrs := range cfg.Groups {
		groups[gid] = addrs
	}
	for _, gid := range gids {
		delete(groups, gid)
	}
	return Config{
		Num:    cfg.Num + 1,
		Groups: groups,
		Shards: rebalance(cfg.Shards, groups),
	}
}

// Move reassigns exactly one shard to gid and rebalances nothing else. It's
// the controller's explicit-placement escape hatch (and a useful test
// fixture) — ordinary membership changes should go through Join/Leave, which
// rebalance automatically and keep the load spread evenly.
func Move(cfg Config, shard, gid int) Config {
	next := Config{Num: cfg.Num + 1, Groups: cfg.Groups, Shards: cfg.Shards}
	next.Shards[shard] = gid
	return next
}

// rebalance assigns every shard to one of liveGroups so that each group ends
// up with floor(NShards/N) or ceil(NShards/N) shards (N = len(liveGroups)),
// moving as few shards as possible away from their current owner in shards.
//
// The algorithm: sort the live group ids (map iteration order is not
// deterministic, so this is the only place a stable order can come from).
// Groups earlier in that order absorb the one-shard remainder, so with 10
// shards over 3 groups the split is 4/3/3, always on the same group. Any
// shard already owned by a group that's either not live or already at its
// target becomes an "orphan"; orphans are handed out, in ascending shard
// order, to groups that are still under target. A shard that was already at
// (or under) its group's target before rebalancing never moves.
func rebalance(shards [NShards]int, liveGroups map[int][]string) [NShards]int {
	gids := make([]int, 0, len(liveGroups))
	for gid := range liveGroups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	if len(gids) == 0 {
		return [NShards]int{} // every shard becomes group 0, i.e. unassigned
	}

	target := NShards / len(gids)
	remainder := NShards % len(gids)
	targetOf := make(map[int]int, len(gids))
	for i, gid := range gids {
		if i < remainder {
			targetOf[gid] = target + 1
		} else {
			targetOf[gid] = target
		}
	}

	owned := make(map[int][]int, len(gids)) // gid -> its shards, ascending
	var orphans []int
	for s, gid := range shards {
		if _, live := liveGroups[gid]; live {
			owned[gid] = append(owned[gid], s)
		} else {
			orphans = append(orphans, s)
		}
	}
	for _, gid := range gids {
		cur := owned[gid]
		if want := targetOf[gid]; len(cur) > want {
			orphans = append(orphans, cur[want:]...)
			owned[gid] = cur[:want]
		}
	}
	sort.Ints(orphans)

	for _, gid := range gids {
		need := targetOf[gid] - len(owned[gid])
		if need <= 0 {
			continue
		}
		take := orphans[:need]
		owned[gid] = append(owned[gid], take...)
		orphans = orphans[need:]
	}

	var result [NShards]int
	for gid, ss := range owned {
		for _, s := range ss {
			result[s] = gid
		}
	}
	return result
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
