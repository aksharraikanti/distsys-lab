package hotshard

import (
	"fmt"
	"sort"
)

// RingConfig is Stage 5's Config rebuilt on a Ring: a versioned snapshot of
// which shards exist (Ring), which group serves each (Owners), and how to
// reach each group (Groups). Ownership lives here rather than in Ring for
// the reason Day 1 gave: a Split changes how many shards exist, a Move
// changes who serves one, and neither should have to be a mutation of the
// structure the other one owns.
//
// Like Config, every operation below is a pure function returning a NEW
// RingConfig with Num+1 and never mutating its input, so every replica of
// the controller applying the same op computes the identical result.
type RingConfig struct {
	Num    int
	Ring   Ring
	Owners map[ShardID]int  // shard -> owning group id
	Groups map[int][]string // group id -> server addresses
}

// NewRingConfig is version 1: ring's shards dealt round-robin to groups in
// ascending gid order, the same deliberately crude placement as Stage 5's
// NewStaticConfig. Rebalancing is what Move (and Day 5) are for.
func NewRingConfig(ring Ring, groups map[int][]string) (RingConfig, error) {
	cfg := RingConfig{Num: 1, Ring: ring, Owners: make(map[ShardID]int), Groups: groups}
	gids := make([]int, 0, len(groups))
	for gid := range groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	if len(gids) == 0 {
		return RingConfig{}, fmt.Errorf("hotshard: NewRingConfig needs at least one group")
	}
	for i, e := range ring.Entries {
		cfg.Owners[e.Shard] = gids[i%len(gids)]
	}
	if err := cfg.Validate(); err != nil {
		return RingConfig{}, err
	}
	return cfg, nil
}

// SplitConfig splits shard at splitPoint (see Split) and gives BOTH halves
// to the group that already owns shard. No data moves at split time — the
// config says only that one range became two, and every key still resolves
// to the same group it did a version ago. Relocating a half is a separate,
// ordinary Move afterward, the same "assignment first, migration follows"
// split Stage 5 Day 2 drew between deciding placement and moving data.
func SplitConfig(cfg RingConfig, shard ShardID, splitPoint uint32) (RingConfig, ShardID, error) {
	owner, ok := cfg.Owners[shard]
	if !ok {
		return RingConfig{}, 0, fmt.Errorf("hotshard: shard %d has no owner in config %d", shard, cfg.Num)
	}
	ring, newID, err := Split(cfg.Ring, shard, splitPoint)
	if err != nil {
		return RingConfig{}, 0, err
	}
	owners := make(map[ShardID]int, len(cfg.Owners)+1)
	for s, g := range cfg.Owners {
		owners[s] = g
	}
	owners[newID] = owner
	return RingConfig{Num: cfg.Num + 1, Ring: ring, Owners: owners, Groups: cfg.Groups}, newID, nil
}

// MoveRingShard reassigns one shard to gid, touching nothing else.
func MoveRingShard(cfg RingConfig, shard ShardID, gid int) (RingConfig, error) {
	if _, ok := cfg.Owners[shard]; !ok {
		return RingConfig{}, fmt.Errorf("hotshard: shard %d not in config %d", shard, cfg.Num)
	}
	if _, ok := cfg.Groups[gid]; !ok {
		return RingConfig{}, fmt.Errorf("hotshard: group %d not in config %d", gid, cfg.Num)
	}
	owners := make(map[ShardID]int, len(cfg.Owners))
	for s, g := range cfg.Owners {
		owners[s] = g
	}
	owners[shard] = gid
	return RingConfig{Num: cfg.Num + 1, Ring: cfg.Ring, Owners: owners, Groups: cfg.Groups}, nil
}

// Midpoint returns the split point halfway through shard's range — Day 4's
// starting policy (a load-weighted point is future work). The range may
// wrap past the top of the hash space, so width is computed mod 2^32; a
// one-shard ring owns the whole space, width 2^32. Errors if the range is
// too narrow to have an interior point.
func Midpoint(ring Ring, shard ShardID) (uint32, error) {
	for i, e := range ring.Entries {
		if e.Shard != shard {
			continue
		}
		width := uint64(rangeEnd(ring, i)) - uint64(e.Start)
		if rangeEnd(ring, i) <= e.Start { // wraps (or sole shard: end == start)
			width += 1 << 32
		}
		if width < 2 {
			return 0, fmt.Errorf("hotshard: shard %d's range is too narrow to split", shard)
		}
		return uint32((uint64(e.Start) + width/2) % (1 << 32)), nil
	}
	return 0, fmt.Errorf("hotshard: shard %d not found in ring", shard)
}

// Validate checks the ring is well-formed, every shard has exactly one
// owner, and every owner is a known group with at least one address.
func (c RingConfig) Validate() error {
	if err := c.Ring.Validate(); err != nil {
		return err
	}
	if len(c.Owners) != len(c.Ring.Entries) {
		return fmt.Errorf("hotshard: %d owners for %d shards", len(c.Owners), len(c.Ring.Entries))
	}
	for _, e := range c.Ring.Entries {
		gid, ok := c.Owners[e.Shard]
		if !ok {
			return fmt.Errorf("hotshard: shard %d has no owner", e.Shard)
		}
		if len(c.Groups[gid]) == 0 {
			return fmt.Errorf("hotshard: shard %d's owner, group %d, is unknown or has no addresses", e.Shard, gid)
		}
	}
	return nil
}

// Lookup returns the group serving key under this config.
func (c RingConfig) Lookup(key string) int {
	return c.Owners[RingAssign(c.Ring, key)]
}
