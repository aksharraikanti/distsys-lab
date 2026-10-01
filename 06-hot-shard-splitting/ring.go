// Package hotshard extends Stage 5's sharded KV store with the one
// capability it's missing: splitting a shard that's carrying more than its
// share of load into two, each independently assignable afterward. See
// 06-hot-shard-splitting/README.md for concept notes and
// 06-hot-shard-splitting/TASKS.md for the day-by-day build plan this file
// follows.
package hotshard

import (
	"fmt"
	"hash/fnv"
	"sort"
)

// ShardID stably identifies one shard — a range of hash space — independent
// of where that range currently sits or which group currently serves it.
// Stage 5's shard identity WAS its array index into a fixed-size Shards
// array; that doesn't work once shards can be created by splitting, so this
// stage gives each shard its own id, assigned once at creation (Day 1's
// NewRing, or a later Split) and never reused or renumbered — Day 2's load
// tracking and Day 3's hot-shard detection both need something stable to
// key their reports against, across a Move that reassigns a shard to a
// different group without touching its identity at all.
type ShardID int

// HashKey maps a key to a point on the ring. Pure function, identical for
// every caller forever, for the same reason Stage 5's Key2Shard had to be:
// two callers that disagreed about a key's position could disagree about
// which shard owns it. FNV-1a for the same reason Stage 5 chose it over a
// first-byte hash — real keys share prefixes ("user:", "session:"), and a
// hash that only looked at the first byte would cluster them all in one
// place on the ring instead of spreading them.
func HashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

// RingEntry is one shard's starting point on the ring: the shard identified
// by Shard owns every point from Start up to (but not including) whichever
// point comes next in the ring — see Ring's own doc comment for how "next"
// wraps.
type RingEntry struct {
	Start uint32
	Shard ShardID
}

// Ring partitions the ENTIRE hash space [0, 2^32) into disjoint ranges, one
// per shard — a ring, not a line, because the space wraps: the entry with
// the HIGHEST Start owns everything from there up to the top of the space
// AND wraps back around to 0, up to (but not including) whichever entry has
// the LOWEST Start. There is no first or last shard, which is exactly the
// property that makes Split (below) only ever touch the one range being
// divided: a shard's neighbors are whichever entries are numerically
// adjacent by Start, not by array position, and "the space before entry 0"
// is never a real gap — it's already covered by the entry that wraps there.
//
// Entries must be kept sorted ascending by Start for RingAssign's lookup to
// work — NewRing and Split both maintain that; a Ring built by hand (as
// property tests here do, to probe specific scenarios) has to maintain it
// itself, the same obligation Stage 5's own Config.Shards array invariants
// placed on whoever constructed one directly.
type Ring struct {
	Entries []RingEntry
}

// NewRing returns a ring with n shards, evenly spaced around the hash space
// — the simplest possible starting point, mirroring Stage 5's own
// NewStaticConfig. It's deliberately not trying to be smart: nothing has
// been OBSERVED yet for it to react to. Splitting (Day 4), driven by Day
// 2/3's load tracking and detection, is what actually responds to a real
// workload; this just bootstraps a ring with nothing to go on.
//
// 2^32 doesn't divide evenly by every n — the remainder is absorbed by
// whichever shard ends up with the highest Start, the same way Stage 5's
// own round-robin left one group with an extra shard when NShards didn't
// divide evenly by the group count.
func NewRing(n int) Ring {
	if n <= 0 {
		panic(fmt.Sprintf("hotshard: NewRing requires n >= 1, got %d", n))
	}
	width := (uint64(1) << 32) / uint64(n)
	entries := make([]RingEntry, n)
	for i := 0; i < n; i++ {
		entries[i] = RingEntry{Start: uint32(uint64(i) * width), Shard: ShardID(i + 1)}
	}
	return Ring{Entries: entries}
}

// NextShardID returns a ShardID not already used anywhere in ring — pure
// function of ring's own contents, so every replica of a future
// replicated layer (once this stage wires splitting into one, the same way
// Stage 5 Day 3 wired Join/Leave/Move into Ctrler) computes the identical
// next id for a Split without needing to coordinate on it separately.
func NextShardID(ring Ring) ShardID {
	max := ShardID(0)
	for _, e := range ring.Entries {
		if e.Shard > max {
			max = e.Shard
		}
	}
	return max + 1
}

// RingAssign returns the shard that owns key under ring. The lookup is
// exactly what makes this a ring rather than a sorted list with a gap
// before its first element: find the last entry (in Start order) whose
// Start is <= hash(key); if hash(key) is smaller than EVERY stored Start,
// that "falls before the first entry" only on a line — on a ring it wraps
// to the entry with the HIGHEST Start, which is exactly who owns that
// stretch of the space.
func RingAssign(ring Ring, key string) ShardID {
	return ringAssignPoint(ring, HashKey(key))
}

// ringAssignPoint is RingAssign's actual lookup, factored out so the wrap
// case can be tested against exact, hand-picked points instead of having to
// find a key whose hash happens to land there — and NewRing always starts
// its first entry at exactly 0, so no key's hash could ever exercise the
// wrap branch through RingAssign alone; only a hand-built Ring (as a test
// can construct, even though NewRing itself never produces one) with a
// nonzero lowest Start can.
func ringAssignPoint(ring Ring, h uint32) ShardID {
	n := len(ring.Entries)
	if n == 0 {
		panic("hotshard: RingAssign called on an empty ring")
	}
	i := sort.Search(n, func(i int) bool { return ring.Entries[i].Start > h })
	if i == 0 {
		return ring.Entries[n-1].Shard
	}
	return ring.Entries[i-1].Shard
}

// rangeEnd returns the exclusive end of ring.Entries[idx]'s range: the
// Start of whichever entry comes next in ring order, wrapping to
// Entries[0].Start if idx is the last entry — see Ring's own doc comment
// for why that wrap is correct, not a special case bolted on.
func rangeEnd(ring Ring, idx int) uint32 {
	next := (idx + 1) % len(ring.Entries)
	return ring.Entries[next].Start
}

// inRangeExclusive reports whether point falls in the half-open interval
// [start, end), where end may be numerically LESS than start — meaning this
// is the wrapping range that covers the top of the space and 0 together.
func inRangeExclusive(start, end, point uint32) bool {
	if start < end {
		return point >= start && point < end
	}
	return point >= start || point < end
}

// Split divides shard's current range into two at splitPoint, which must
// fall STRICTLY inside that range — not equal to shard's own Start (that
// would just rename it, not divide anything) and not outside it (that
// would be dividing a DIFFERENT shard's territory). The lower half keeps
// shard's existing id and Start; the upper half, from splitPoint onward,
// gets a freshly minted id from NextShardID. Every key that mapped to some
// OTHER shard before the split keeps mapping to that exact same shard
// afterward — the one property a fixed Key2Shard %% NShards could never
// offer, and the entire reason this stage moved to a ring in the first
// place.
//
// Split validates its own arguments and returns an error for a bad
// splitPoint, rather than trusting the caller the way Stage 5's pure
// Join/Leave/Move do — those were always called from behind Ctrler's own
// already-validating RPC boundary; this stage hasn't built an equivalent
// boundary yet (that's later days' job), so Split is the only thing
// standing between a bad split point and a corrupted ring right now.
func Split(ring Ring, shard ShardID, splitPoint uint32) (Ring, ShardID, error) {
	idx := -1
	for i, e := range ring.Entries {
		if e.Shard == shard {
			idx = i
			break
		}
	}
	if idx == -1 {
		return Ring{}, 0, fmt.Errorf("hotshard: shard %d not found in ring", shard)
	}

	start := ring.Entries[idx].Start
	end := rangeEnd(ring, idx)
	if splitPoint == start || !inRangeExclusive(start, end, splitPoint) {
		return Ring{}, 0, fmt.Errorf("hotshard: split point %d is not strictly inside shard %d's range [%d, %d)", splitPoint, shard, start, end)
	}

	newID := NextShardID(ring)
	entries := make([]RingEntry, len(ring.Entries)+1)
	copy(entries, ring.Entries)
	entries[len(ring.Entries)] = RingEntry{Start: splitPoint, Shard: newID}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Start < entries[j].Start })

	return Ring{Entries: entries}, newID, nil
}

// Validate checks that ring actually partitions the hash space: every
// shard id appears exactly once, and entries are strictly sorted ascending
// by Start — the one invariant RingAssign's binary search depends on, and
// Split's own insert-then-sort always restores afterward, but a Ring built
// by hand elsewhere in this package's tests might not.
func (r Ring) Validate() error {
	if len(r.Entries) == 0 {
		return fmt.Errorf("hotshard: ring has no shards")
	}
	seen := make(map[ShardID]bool, len(r.Entries))
	for i, e := range r.Entries {
		if seen[e.Shard] {
			return fmt.Errorf("hotshard: shard %d appears more than once in the ring", e.Shard)
		}
		seen[e.Shard] = true
		if i > 0 && r.Entries[i-1].Start >= e.Start {
			return fmt.Errorf("hotshard: ring entries not strictly sorted ascending by Start (entry %d Start=%d, entry %d Start=%d)",
				i-1, r.Entries[i-1].Start, i, e.Start)
		}
	}
	return nil
}
