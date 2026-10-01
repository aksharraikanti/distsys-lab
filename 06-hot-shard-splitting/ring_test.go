package hotshard

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestHashKeyIsDeterministicAndSpreadsPrefixedKeys(t *testing.T) {
	ring := NewRing(10)
	const n = 10000
	counts := make(map[ShardID]int)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("user:%d", i)
		s1 := RingAssign(ring, key)
		s2 := RingAssign(ring, key)
		if s1 != s2 {
			t.Fatalf("RingAssign(%q) returned %d then %d: it must be a pure function", key, s1, s2)
		}
		counts[s1]++
	}
	mean := float64(n) / float64(len(ring.Entries))
	for _, e := range ring.Entries {
		c := counts[e.Shard]
		t.Logf("shard %d: %5d keys (%.2fx the mean)", e.Shard, c, float64(c)/mean)
		if float64(c) < 0.8*mean || float64(c) > 1.2*mean {
			t.Fatalf("shard %d holds %d of %d keys (mean %.0f): the hash is not spreading prefixed keys evenly", e.Shard, c, n, mean)
		}
	}
}

func TestNewRingIsBalancedAndValid(t *testing.T) {
	for _, n := range []int{1, 2, 3, 7, 10, 23} {
		ring := NewRing(n)
		if err := ring.Validate(); err != nil {
			t.Fatalf("NewRing(%d) produced an invalid ring: %v", n, err)
		}
		if len(ring.Entries) != n {
			t.Fatalf("NewRing(%d) produced %d entries, want %d", n, len(ring.Entries), n)
		}
		want := (uint64(1) << 32) / uint64(n)
		for i, e := range ring.Entries {
			// Plain uint32 subtraction wraps correctly mod 2^32 on its
			// own — the only case it can't express is "the whole ring"
			// (n == 1, where Start and the wrapped end are both 0, so the
			// subtraction gives 0 instead of 2^32).
			got := uint64(rangeEnd(ring, i) - e.Start)
			if got == 0 {
				got = uint64(1) << 32
			}
			// The highest-Start entry absorbs 2^32's remainder mod n, so
			// it can be up to n-1 wider than the rest — allow for that
			// instead of demanding exact equality.
			if got < want || got > want+uint64(n) {
				t.Fatalf("NewRing(%d) entry %d has width %d, want approximately %d", n, i, got, want)
			}
		}
	}
}

// TestRingPartitionsWithNoGapsOrOverlaps is this day's own named property:
// every point on the ring belongs to EXACTLY one shard, never zero (a gap)
// and never more than one (an overlap). Checked directly against sampled
// ring positions, not just string keys, so it exercises exact boundary
// points (a Start itself, and the point just before it) along with random
// interior points.
func TestRingPartitionsWithNoGapsOrOverlaps(t *testing.T) {
	ring := NewRing(7)
	rng := rand.New(rand.NewSource(1))

	var points []uint32
	for _, e := range ring.Entries {
		points = append(points, e.Start, e.Start-1) // the boundary itself, and just before it
	}
	for i := 0; i < 5000; i++ {
		points = append(points, rng.Uint32())
	}

	for _, p := range points {
		owners := 0
		for i, e := range ring.Entries {
			if inRangeExclusive(e.Start, rangeEnd(ring, i), p) {
				owners++
			}
		}
		if owners != 1 {
			t.Fatalf("point %d is claimed by %d shards (want exactly 1): ring=%+v", p, owners, ring.Entries)
		}
	}
}

func TestSplitRejectsAPointOutsideTheShardsRange(t *testing.T) {
	ring := NewRing(4) // starts at 0, 1*2^30, 2*2^30, 3*2^30
	shard := ring.Entries[1].Shard

	// Exactly at the shard's own Start: doesn't divide anything.
	if _, _, err := Split(ring, shard, ring.Entries[1].Start); err == nil {
		t.Fatal("Split at a shard's own Start should be rejected (it wouldn't divide anything)")
	}
	// Inside a DIFFERENT shard's range entirely.
	if _, _, err := Split(ring, shard, ring.Entries[3].Start+1); err == nil {
		t.Fatal("Split at a point outside the named shard's range should be rejected")
	}
	// An unknown shard id.
	if _, _, err := Split(ring, NextShardID(ring), ring.Entries[1].Start+1); err == nil {
		t.Fatal("Split naming a shard id not in the ring should be rejected")
	}
}

func TestSplitProducesTwoValidRangesCoveringTheOriginal(t *testing.T) {
	ring := NewRing(3)
	shard := ring.Entries[0].Shard
	start := ring.Entries[0].Start
	end := rangeEnd(ring, 0)
	mid := start + (end-start)/2 // no wrap for entry 0 in a 3-way NewRing

	next, newShard, err := Split(ring, shard, mid)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}
	if err := next.Validate(); err != nil {
		t.Fatalf("Split produced an invalid ring: %v", err)
	}
	if len(next.Entries) != len(ring.Entries)+1 {
		t.Fatalf("Split should add exactly one entry: got %d, want %d", len(next.Entries), len(ring.Entries)+1)
	}
	if newShard == shard {
		t.Fatal("the new half must get a DIFFERENT shard id than the original")
	}

	// The two halves together must cover EXACTLY the original range, no
	// more and no less: [start, mid) stays with shard, [mid, end) goes to
	// newShard.
	foundOld, foundNew := false, false
	for i, e := range next.Entries {
		if e.Shard == shard {
			foundOld = true
			if e.Start != start || rangeEnd(next, i) != mid {
				t.Fatalf("original shard's range after split = [%d, %d), want [%d, %d)", e.Start, rangeEnd(next, i), start, mid)
			}
		}
		if e.Shard == newShard {
			foundNew = true
			if e.Start != mid || rangeEnd(next, i) != end {
				t.Fatalf("new shard's range after split = [%d, %d), want [%d, %d)", e.Start, rangeEnd(next, i), mid, end)
			}
		}
	}
	if !foundOld || !foundNew {
		t.Fatalf("expected to find both shard %d and %d in the post-split ring", shard, newShard)
	}
}

// TestSplitDoesNotChangeOwnershipOutsideTheSplitRange is this day's other
// named property, and the entire reason this stage moved off a fixed
// Key2Shard %% NShards: splitting one shard must never change which shard
// ANY key outside that shard's own range belongs to.
func TestSplitDoesNotChangeOwnershipOutsideTheSplitRange(t *testing.T) {
	ring := NewRing(5)
	target := ring.Entries[2].Shard
	start := ring.Entries[2].Start
	end := rangeEnd(ring, 2)
	mid := start + (end-start)/2

	before := make(map[string]ShardID)
	const n = 20000
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key-%d", i)
		before[key] = RingAssign(ring, key)
	}

	after, newShard, err := Split(ring, target, mid)
	if err != nil {
		t.Fatalf("Split failed: %v", err)
	}

	changed, changedButShouldnt := 0, 0
	for key, oldShard := range before {
		newOwner := RingAssign(after, key)
		if oldShard != target {
			// This key was never in the split shard's range at all — its
			// owner must be byte-for-byte identical to before.
			if newOwner != oldShard {
				changedButShouldnt++
			}
			continue
		}
		// This key WAS in the split shard's range — its new owner must be
		// EITHER the original shard (if it fell in the lower half) or the
		// new one (if it fell in the upper half), never anything else.
		if newOwner != target && newOwner != newShard {
			t.Fatalf("key %q was on shard %d before the split; after, it's on shard %d, neither the original nor the new half", key, oldShard, newOwner)
		}
		changed++
	}
	if changedButShouldnt > 0 {
		t.Fatalf("%d keys that were NOT on the split shard changed owner anyway", changedButShouldnt)
	}
	if changed == 0 {
		t.Fatal("no keys landed on the split shard at all — this test isn't exercising anything")
	}
	t.Logf("%d of %d keys were on the split shard and redistributed between its two halves; the other %d were completely unaffected", changed, n, n-changed)
}

func TestNextShardIDNeverCollides(t *testing.T) {
	ring := NewRing(3)
	id := NextShardID(ring)
	for _, e := range ring.Entries {
		if e.Shard == id {
			t.Fatalf("NextShardID returned %d, which already exists in the ring", id)
		}
	}

	// After several splits, NextShardID must still never collide with
	// anything already assigned, including ids minted by EARLIER splits.
	for i := 0; i < 5; i++ {
		target := ring.Entries[0].Shard
		start := ring.Entries[0].Start
		end := rangeEnd(ring, 0)
		mid := start + (end-start)/2
		if mid == start {
			break // range too narrow to keep splitting meaningfully
		}
		next, newID, err := Split(ring, target, mid)
		if err != nil {
			t.Fatalf("Split failed on round %d: %v", i, err)
		}
		for _, e := range ring.Entries {
			if e.Shard == newID {
				t.Fatalf("round %d: new shard id %d collided with an existing one", i, newID)
			}
		}
		ring = next
	}
}

func TestRingValidateRejectsUnsortedOrDuplicateEntries(t *testing.T) {
	good := NewRing(3)
	if err := good.Validate(); err != nil {
		t.Fatalf("a good ring was rejected: %v", err)
	}

	unsorted := Ring{Entries: []RingEntry{
		{Start: 100, Shard: 1},
		{Start: 50, Shard: 2},
	}}
	if err := unsorted.Validate(); err == nil {
		t.Fatal("an unsorted ring must be rejected")
	}

	duplicate := Ring{Entries: []RingEntry{
		{Start: 0, Shard: 1},
		{Start: 100, Shard: 1},
	}}
	if err := duplicate.Validate(); err == nil {
		t.Fatal("a ring with the same shard id twice must be rejected")
	}

	if err := (Ring{}).Validate(); err == nil {
		t.Fatal("an empty ring must be rejected")
	}
}

// TestRingAssignWrapsPastTheTopBackToZero exercises the wrap case directly,
// against exact hand-picked points rather than hoping some key's hash lands
// there: NewRing always starts its first entry at exactly 0, so no key
// could ever reach the wrap branch through a NewRing-built ring. A
// hand-built ring whose lowest Start ISN'T 0 is the only way to prove a
// point before that Start correctly wraps to the shard with the HIGHEST
// Start, rather than falling through to the lowest one.
func TestRingAssignWrapsPastTheTopBackToZero(t *testing.T) {
	ring := Ring{Entries: []RingEntry{
		{Start: 1000, Shard: 10},
		{Start: 2_000_000_000, Shard: 20},
	}}
	if err := ring.Validate(); err != nil {
		t.Fatalf("hand-built ring should validate: %v", err)
	}

	for _, p := range []uint32{0, 500, 999} {
		if owner := ringAssignPoint(ring, p); owner != 20 {
			t.Fatalf("point %d (before the lowest Start) should wrap to shard 20 (the highest Start), got %d", p, owner)
		}
	}
	if owner := ringAssignPoint(ring, 1500); owner != 10 {
		t.Fatalf("point 1500 should belong to shard 10, got %d", owner)
	}
	if owner := ringAssignPoint(ring, 2_000_000_001); owner != 20 {
		t.Fatalf("point 2000000001 should belong to shard 20, got %d", owner)
	}
}
