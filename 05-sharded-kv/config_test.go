package shardkv

import (
	"fmt"
	"math/rand"
	"testing"
)

// checkBalanced fails unless every live group's shard count is within 1 of
// every other's — the property rebalance is supposed to hold after every
// single Join/Leave, not just eventually.
func checkBalanced(t *testing.T, cfg Config) {
	t.Helper()
	if len(cfg.Groups) == 0 {
		return
	}
	counts := make(map[int]int, len(cfg.Groups))
	for gid := range cfg.Groups {
		counts[gid] = 0
	}
	for s, gid := range cfg.Shards {
		if _, live := cfg.Groups[gid]; !live {
			t.Fatalf("shard %d is owned by group %d, which is not in Groups %v", s, gid, cfg.Groups)
		}
		counts[gid]++
	}
	min, max := NShards, 0
	for _, n := range counts {
		if n < min {
			min = n
		}
		if n > max {
			max = n
		}
	}
	if max-min > 1 {
		t.Fatalf("load not balanced within 1 shard: counts %v (config %+v)", counts, cfg)
	}
}

func countMoved(before, after [NShards]int) int {
	n := 0
	for s := range before {
		if before[s] != after[s] {
			n++
		}
	}
	return n
}

func addrsFor(gid int) []string { return []string{fmt.Sprintf("host-%d:0", gid)} }

func TestJoinBootstrapsFromEmptyConfig(t *testing.T) {
	cfg := Join(Config{}, map[int][]string{1: addrsFor(1), 2: addrsFor(2), 3: addrsFor(3)})
	if cfg.Num != 1 {
		t.Fatalf("Join from the zero Config should produce Num 1, got %d", cfg.Num)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("bootstrapped config should validate: %v", err)
	}
	checkBalanced(t, cfg)
}

func TestJoinIsDeterministic(t *testing.T) {
	base := Join(Config{}, map[int][]string{7: addrsFor(7), 3: addrsFor(3), 5: addrsFor(5)})
	first := Join(base, map[int][]string{2: addrsFor(2), 9: addrsFor(9)})
	for i := 0; i < 50; i++ { // map iteration order is random; the result must not be
		if got := Join(base, map[int][]string{2: addrsFor(2), 9: addrsFor(9)}); got.Shards != first.Shards {
			t.Fatalf("Join gave different assignments on repeat calls: %v vs %v", got.Shards, first.Shards)
		}
	}
}

func TestLeaveIsDeterministic(t *testing.T) {
	base := Join(Config{}, map[int][]string{1: addrsFor(1), 2: addrsFor(2), 3: addrsFor(3), 4: addrsFor(4), 5: addrsFor(5)})
	first := Leave(base, []int{2, 4})
	for i := 0; i < 50; i++ {
		if got := Leave(base, []int{2, 4}); got.Shards != first.Shards {
			t.Fatalf("Leave gave different assignments on repeat calls: %v vs %v", got.Shards, first.Shards)
		}
	}
}

// TestLeaveEveryGroupLeavesShardsUnassigned checks the documented edge case:
// removing every group can't leave a routable config, and Validate must catch
// it rather than silently pointing shards at nothing.
func TestLeaveEveryGroupLeavesShardsUnassigned(t *testing.T) {
	base := Join(Config{}, map[int][]string{1: addrsFor(1), 2: addrsFor(2)})
	after := Leave(base, []int{1, 2})
	for s, gid := range after.Shards {
		if gid != 0 {
			t.Fatalf("shard %d should be unassigned (group 0) once every group has left, got group %d", s, gid)
		}
	}
	if err := after.Validate(); err == nil {
		t.Fatal("a config with no groups left must fail Validate")
	}
}

// TestMoveTouchesExactlyOneShard is Move's whole contract: unlike Join/Leave
// it must not rebalance anything else, even if that leaves the config
// unbalanced.
func TestMoveTouchesExactlyOneShard(t *testing.T) {
	base := Join(Config{}, map[int][]string{1: addrsFor(1), 2: addrsFor(2), 3: addrsFor(3)})
	target := (base.Shards[0] % 3) + 1 // some group, possibly the same one shard 0 already has
	after := Move(base, 0, target)

	if after.Num != base.Num+1 {
		t.Fatalf("Move should bump Num by 1, got %d -> %d", base.Num, after.Num)
	}
	if after.Shards[0] != target {
		t.Fatalf("Move(0, %d) left shard 0 owned by %d", target, after.Shards[0])
	}
	for s := 1; s < NShards; s++ {
		if after.Shards[s] != base.Shards[s] {
			t.Fatalf("Move moved shard %d too: %d -> %d", s, base.Shards[s], after.Shards[s])
		}
	}
}

// TestRebalanceMovesOnlyWhatItMustOnJoin is the minimal-movement property
// pinned to a concrete, hand-checkable case: starting from 3 balanced groups
// (4/3/3 over 10 shards) and adding a 4th, the new target is 3/3/2/2 — only
// the two groups that were over their new target should lose shards, and
// only the group at its old target that's now over should shrink by exactly
// the amount needed to feed the new group. Nothing already at-or-under target
// should move.
func TestRebalanceMovesOnlyWhatItMustOnJoin(t *testing.T) {
	before := Join(Config{}, map[int][]string{1: addrsFor(1), 2: addrsFor(2), 3: addrsFor(3)})
	checkBalanced(t, before)

	after := Join(before, map[int][]string{4: addrsFor(4)})
	checkBalanced(t, after)

	moved := countMoved(before.Shards, after.Shards)
	// New group 4's target is 10/4=2 (with remainder 2, so groups 1 and 2 get
	// 3, groups 3 and 4 get 2). Group 4 must receive exactly 2 shards, and
	// since nothing else needs to shrink below its own target to supply them,
	// exactly 2 shards should have moved: no more, no less.
	if moved != 2 {
		t.Fatalf("adding a 4th group to a balanced 3-group config moved %d shards, want exactly 2 (before=%v after=%v)", moved, before.Shards, after.Shards)
	}
}

// TestJoinLeaveSequencesStayBalancedAndRoutable runs many random join/leave
// sequences and checks, after every single step, that the config is still
// fully routable and balanced to within 1 shard. This is the property test
// TASKS.md asks for: the specific sequence doesn't matter, the invariant must
// hold after every step of every sequence.
func TestJoinLeaveSequencesStayBalancedAndRoutable(t *testing.T) {
	for trial := 0; trial < 200; trial++ {
		rng := rand.New(rand.NewSource(int64(trial)))
		cfg := Config{}
		live := map[int]bool{}
		nextGid := 1

		steps := 5 + rng.Intn(15)
		for step := 0; step < steps; step++ {
			// Bias toward Join when there are 0 or 1 live groups, so Leave
			// has something to remove; otherwise pick randomly.
			doJoin := len(live) < 2 || rng.Intn(2) == 0

			if doJoin {
				n := 1 + rng.Intn(3)
				newGroups := make(map[int][]string, n)
				for i := 0; i < n; i++ {
					gid := nextGid
					nextGid++
					newGroups[gid] = addrsFor(gid)
					live[gid] = true
				}
				cfg = Join(cfg, newGroups)
			} else {
				var gids []int
				for gid := range live {
					gids = append(gids, gid)
				}
				rng.Shuffle(len(gids), func(i, j int) { gids[i], gids[j] = gids[j], gids[i] })
				n := 1 + rng.Intn(len(gids))
				leaving := gids[:n]
				for _, gid := range leaving {
					delete(live, gid)
				}
				cfg = Leave(cfg, leaving)
			}

			if len(live) == 0 {
				continue // nothing left to route; Validate is expected to reject this
			}
			if err := cfg.Validate(); err != nil {
				t.Fatalf("trial %d step %d: config failed to validate after %d live groups: %v (cfg=%+v)", trial, step, len(live), err, cfg)
			}
			checkBalanced(t, cfg)
		}
	}
}
