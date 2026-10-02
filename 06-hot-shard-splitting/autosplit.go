package hotshard

import (
	"fmt"
	"sort"
)

// AutoSplitter closes the loop Days 1-4 built the parts of: read load
// (Day 2), decide what's hot (Day 3), propose a Split (Day 4), then use a
// plain Stage 5-style Move to relocate half of it. It doesn't run itself —
// Step is one decision, called on whatever cadence the caller polls load
// at (Snapshot's window IS that cadence, per Day 2), which keeps the
// whole thing deterministic to test.
type AutoSplitter struct {
	Ctrl   RingAPI
	Policy HotPolicy
	// Loads returns the merged per-shard load for the window since the
	// last call (MergeLoads over every group's LoadTracker.Snapshot).
	Loads func() map[ShardID]int
	// MaxShards caps how large the ring may grow. Without it, one hot KEY
	// (a single point on the ring no split can divide) would be re-flagged
	// forever, each split just carving a narrower range around it.
	MaxShards int
}

// StepResult describes what one Step did. The zero value means "nothing
// was hot."
type StepResult struct {
	Split    ShardID // the shard that was split, 0 if none
	NewShard ShardID // the upper half minted by the split
	MovedTo  int     // group the upper half was moved to, 0 if it stayed
}

// Step performs at most ONE split. Splitting only the hottest flagged
// shard per step is deliberate: every other load number in the window was
// measured against the old ring, and acting on several of them at once
// would be acting on a picture the first split already made stale. The
// next window measures the new ring, and the next Step sees what's still
// hot.
func (a *AutoSplitter) Step() (StepResult, error) {
	cfg, err := a.Ctrl.Query()
	if err != nil {
		return StepResult{}, err
	}
	shards := make([]ShardID, len(cfg.Ring.Entries))
	for i, e := range cfg.Ring.Entries {
		shards[i] = e.Shard
	}
	loads := a.Loads()
	hot := DetectHot(loads, shards, a.Policy)
	if len(hot) == 0 || (a.MaxShards > 0 && len(shards) >= a.MaxShards) {
		return StepResult{}, nil
	}

	victim := hot[0]
	point, err := Midpoint(cfg.Ring, victim)
	if err != nil {
		return StepResult{}, nil // range too narrow to divide; nothing to do
	}
	newID := NextShardID(cfg.Ring)
	owner := cfg.Owners[victim]
	if err := a.Ctrl.Split(victim, point); err != nil {
		return StepResult{}, err
	}
	res := StepResult{Split: victim, NewShard: newID}

	// Confirm the new half is the one we minted, still on the original
	// owner, before moving it: if someone else changed the ring between
	// our Query and the Split committing, newID may not be ours.
	after, err := a.Ctrl.Query()
	if err != nil {
		return res, err
	}
	if after.Owners[newID] != owner || ringStart(after.Ring, newID) != point {
		return res, nil
	}

	target := coolestGroup(cfg, loads)
	if target == owner || target == 0 {
		return res, nil
	}
	if err := a.Ctrl.Move(newID, target); err != nil {
		return res, fmt.Errorf("moving shard %d to group %d: %w", newID, target, err)
	}
	res.MovedTo = target
	return res, nil
}

func ringStart(r Ring, s ShardID) uint32 {
	for _, e := range r.Entries {
		if e.Shard == s {
			return e.Start
		}
	}
	return 0
}

// coolestGroup returns the group carrying the least load under cfg's
// ownership, lowest gid on ties. Groups owning no shards count as zero —
// an idle group is exactly where hot load should go.
func coolestGroup(cfg RingConfig, loads map[ShardID]int) int {
	byGroup := make(map[int]int, len(cfg.Groups))
	for gid := range cfg.Groups {
		byGroup[gid] = 0
	}
	for s, gid := range cfg.Owners {
		byGroup[gid] += loads[s]
	}
	gids := make([]int, 0, len(byGroup))
	for gid := range byGroup {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	best := 0
	for _, gid := range gids {
		if best == 0 || byGroup[gid] < byGroup[best] {
			best = gid
		}
	}
	return best
}
