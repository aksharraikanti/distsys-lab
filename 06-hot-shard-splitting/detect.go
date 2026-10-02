package hotshard

import "sort"

// HotPolicy decides what "hot" means. Hotness is relative: a shard is hot
// when its load exceeds Factor times the mean load across ALL shards, so
// the same shard that's hot on a quiet cluster isn't on a busy one.
type HotPolicy struct {
	// Factor is the multiple of the mean a shard must exceed (strictly) to
	// be flagged. Must be > 1 to mean anything; a Factor of 2 flags
	// shards carrying more than twice an even share.
	Factor float64
	// MinLoad is a floor on the shard's own count, so near-idle clusters
	// (1 request against a mean of 0.1) don't flag noise. Zero disables it.
	MinLoad int
}

// MergeLoads sums per-group snapshots into one load per shard. Each group
// only reports shards it served, so a shard that moved mid-window can
// legitimately appear in two reports; its real load is the sum.
func MergeLoads(reports ...map[ShardID]int) map[ShardID]int {
	total := make(map[ShardID]int)
	for _, r := range reports {
		for s, n := range r {
			total[s] += n
		}
	}
	return total
}

// DetectHot returns the shards in `shards` whose load in `loads` is hot
// under policy, hottest first (ties broken by ascending ShardID so the
// result is deterministic).
//
// The mean is taken over every shard in `shards`, not just those present
// in `loads`: Snapshot omits shards with zero traffic, and dropping them
// from the denominator would inflate the mean and hide a genuinely
// skewed cluster. Entries in `loads` for shards not in `shards` (a stale
// report from before a split) are ignored.
func DetectHot(loads map[ShardID]int, shards []ShardID, policy HotPolicy) []ShardID {
	if len(shards) == 0 {
		return nil
	}
	total := 0
	for _, s := range shards {
		total += loads[s]
	}
	mean := float64(total) / float64(len(shards))
	threshold := policy.Factor * mean

	var hot []ShardID
	for _, s := range shards {
		n := loads[s]
		if n >= policy.MinLoad && float64(n) > threshold && n > 0 {
			hot = append(hot, s)
		}
	}
	sort.Slice(hot, func(i, j int) bool {
		if loads[hot[i]] != loads[hot[j]] {
			return loads[hot[i]] > loads[hot[j]]
		}
		return hot[i] < hot[j]
	})
	return hot
}
