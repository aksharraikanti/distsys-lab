package hotshard

import (
	"reflect"
	"testing"
)

var pol = HotPolicy{Factor: 2}

func TestDetectHotFlagsOutlier(t *testing.T) {
	loads := map[ShardID]int{0: 10, 1: 10, 2: 10, 3: 100}
	got := DetectHot(loads, []ShardID{0, 1, 2, 3}, pol)
	if !reflect.DeepEqual(got, []ShardID{3}) {
		t.Fatalf("got %v, want [3]", got)
	}
}

func TestDetectHotEvenLoadFlagsNothing(t *testing.T) {
	loads := map[ShardID]int{0: 50, 1: 50, 2: 50, 3: 50}
	if got := DetectHot(loads, []ShardID{0, 1, 2, 3}, pol); len(got) != 0 {
		t.Fatalf("even load flagged %v", got)
	}
}

// Relative, not absolute: the same count is hot on a quiet cluster and
// not on a busy one.
func TestDetectHotIsRelative(t *testing.T) {
	shards := []ShardID{0, 1, 2, 3}
	quiet := map[ShardID]int{0: 1, 1: 1, 2: 1, 3: 100}
	busy := map[ShardID]int{0: 100, 1: 100, 2: 100, 3: 100}
	if got := DetectHot(quiet, shards, pol); !reflect.DeepEqual(got, []ShardID{3}) {
		t.Fatalf("quiet: got %v, want [3]", got)
	}
	if got := DetectHot(busy, shards, pol); len(got) != 0 {
		t.Fatalf("busy: got %v, want none", got)
	}
}

// Shards absent from the snapshot count as zero in the mean. Without that,
// a single reporting shard would be its own mean and never look hot.
func TestDetectHotCountsIdleShardsInMean(t *testing.T) {
	loads := map[ShardID]int{3: 40}
	got := DetectHot(loads, []ShardID{0, 1, 2, 3}, pol)
	if !reflect.DeepEqual(got, []ShardID{3}) {
		t.Fatalf("got %v, want [3]", got)
	}
}

func TestDetectHotThresholdIsStrict(t *testing.T) {
	// mean = 20, threshold = 40; shard 0 sits exactly on it.
	loads := map[ShardID]int{0: 40, 1: 40, 2: 0, 3: 0}
	// mean = 20 -> threshold 40 -> 40 is not > 40.
	if got := DetectHot(loads, []ShardID{0, 1, 2, 3}, pol); len(got) != 0 {
		t.Fatalf("exactly-at-threshold flagged %v", got)
	}
}

func TestDetectHotMinLoadFloor(t *testing.T) {
	loads := map[ShardID]int{0: 1}
	shards := []ShardID{0, 1, 2, 3}
	if got := DetectHot(loads, shards, pol); len(got) != 1 {
		t.Fatalf("without floor, got %v, want [0]", got)
	}
	if got := DetectHot(loads, shards, HotPolicy{Factor: 2, MinLoad: 10}); len(got) != 0 {
		t.Fatalf("with floor, got %v, want none", got)
	}
}

func TestDetectHotOrderingAndStaleShards(t *testing.T) {
	loads := map[ShardID]int{0: 100, 1: 300, 2: 300, 9: 5000, 3: 0, 4: 0, 5: 0, 6: 0, 7: 0}
	shards := []ShardID{0, 1, 2, 3, 4, 5, 6, 7}
	got := DetectHot(loads, shards, pol)
	want := []ShardID{1, 2}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v (hottest first, ties by id, shard 9 ignored)", got, want)
	}
}

func TestDetectHotNoShardsOrNoTraffic(t *testing.T) {
	if got := DetectHot(nil, nil, pol); got != nil {
		t.Fatalf("no shards: got %v", got)
	}
	if got := DetectHot(nil, []ShardID{0, 1}, pol); len(got) != 0 {
		t.Fatalf("no traffic: got %v", got)
	}
}

func TestMergeLoadsSumsAcrossGroups(t *testing.T) {
	a := map[ShardID]int{1: 5, 2: 3}
	b := map[ShardID]int{2: 4, 3: 1}
	got := MergeLoads(a, b)
	want := map[ShardID]int{1: 5, 2: 7, 3: 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if a[2] != 3 {
		t.Fatalf("MergeLoads mutated its input")
	}
}
