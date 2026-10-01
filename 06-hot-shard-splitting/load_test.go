package hotshard

import (
	"sync"
	"testing"
)

func TestLoadTrackerCountsPerShard(t *testing.T) {
	lt := NewLoadTracker()
	for i := 0; i < 5; i++ {
		lt.Record(1)
	}
	for i := 0; i < 2; i++ {
		lt.Record(2)
	}

	snap := lt.Snapshot()
	if snap[1] != 5 {
		t.Fatalf("shard 1 count = %d, want 5", snap[1])
	}
	if snap[2] != 2 {
		t.Fatalf("shard 2 count = %d, want 2", snap[2])
	}
	if _, ok := snap[3]; ok {
		t.Fatalf("shard 3 was never recorded against; it should be absent from the snapshot, got %v", snap)
	}
}

// TestLoadTrackerSnapshotResets is the "since the last read, not ever"
// property TASKS.md's own reset-on-read design depends on: a second
// Snapshot with no new Record calls in between must come back empty, not
// repeat the previous counts.
func TestLoadTrackerSnapshotResets(t *testing.T) {
	lt := NewLoadTracker()
	lt.Record(1)
	lt.Record(1)

	first := lt.Snapshot()
	if first[1] != 2 {
		t.Fatalf("first snapshot: shard 1 count = %d, want 2", first[1])
	}

	second := lt.Snapshot()
	if len(second) != 0 {
		t.Fatalf("second snapshot (no Records in between) should be empty, got %v", second)
	}

	lt.Record(1)
	third := lt.Snapshot()
	if third[1] != 1 {
		t.Fatalf("third snapshot: shard 1 count = %d, want 1 (only the one Record since the last Snapshot)", third[1])
	}
}

// TestLoadTrackerRecordIsSafeForConcurrentUse is the actual point of
// building this as a tracker rather than a plain map: many goroutines
// recording real concurrent request traffic must never lose a count.
func TestLoadTrackerRecordIsSafeForConcurrentUse(t *testing.T) {
	lt := NewLoadTracker()
	const goroutines = 50
	const perGoroutine = 200

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				lt.Record(ShardID(j % 3))
			}
		}()
	}
	wg.Wait()

	snap := lt.Snapshot()
	total := 0
	for _, c := range snap {
		total += c
	}
	if want := goroutines * perGoroutine; total != want {
		t.Fatalf("total recorded count = %d, want %d (a count was lost under concurrent Record calls)", total, want)
	}
}
