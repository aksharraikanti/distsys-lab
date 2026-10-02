package hotshard

import (
	"fmt"
	"testing"
)

var testGroups = map[int][]string{1: {"a"}, 2: {"b"}, 3: {"c"}}

func mustConfig(t *testing.T, shards int) RingConfig {
	t.Helper()
	cfg, err := NewRingConfig(NewRing(shards), testGroups)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// The Day 4 property: splitting changes no key's owning GROUP — both halves
// start on the original owner, so a split is invisible to routing.
func TestSplitConfigPreservesEveryKeysGroup(t *testing.T) {
	cfg := mustConfig(t, 4)
	mid, err := Midpoint(cfg.Ring, 2)
	if err != nil {
		t.Fatal(err)
	}
	next, newID, err := SplitConfig(cfg, 2, mid)
	if err != nil {
		t.Fatal(err)
	}
	if next.Num != cfg.Num+1 {
		t.Fatalf("Num = %d, want %d", next.Num, cfg.Num+1)
	}
	if next.Owners[newID] != cfg.Owners[2] {
		t.Fatalf("new half owned by %d, want original owner %d", next.Owners[newID], cfg.Owners[2])
	}
	if err := next.Validate(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("key-%d", i)
		if cfg.Lookup(k) != next.Lookup(k) {
			t.Fatalf("%s moved from group %d to %d across a split", k, cfg.Lookup(k), next.Lookup(k))
		}
	}
}

func TestSplitConfigDoesNotMutateInput(t *testing.T) {
	cfg := mustConfig(t, 4)
	mid, _ := Midpoint(cfg.Ring, 1)
	if _, _, err := SplitConfig(cfg, 1, mid); err != nil {
		t.Fatal(err)
	}
	if len(cfg.Owners) != 4 || len(cfg.Ring.Entries) != 4 || cfg.Num != 1 {
		t.Fatalf("input mutated: %+v", cfg)
	}
}

func TestSplitConfigRejectsBadInput(t *testing.T) {
	cfg := mustConfig(t, 4)
	if _, _, err := SplitConfig(cfg, 99, 5); err == nil {
		t.Fatal("unknown shard accepted")
	}
	if _, _, err := SplitConfig(cfg, 1, cfg.Ring.Entries[0].Start); err == nil {
		t.Fatal("split at the shard's own Start accepted")
	}
	if _, _, err := SplitConfig(cfg, 1, cfg.Ring.Entries[2].Start); err == nil {
		t.Fatal("split point inside a different shard accepted")
	}
}

func TestMoveRingShard(t *testing.T) {
	cfg := mustConfig(t, 4)
	next, err := MoveRingShard(cfg, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	if next.Owners[1] != 3 || next.Num != cfg.Num+1 {
		t.Fatalf("bad move result %+v", next)
	}
	for s, g := range cfg.Owners {
		if s != 1 && next.Owners[s] != g {
			t.Fatalf("shard %d changed owner", s)
		}
	}
	if cfg.Owners[1] == 3 {
		t.Fatal("input mutated")
	}
	if _, err := MoveRingShard(cfg, 1, 42); err == nil {
		t.Fatal("unknown group accepted")
	}
	if _, err := MoveRingShard(cfg, 42, 1); err == nil {
		t.Fatal("unknown shard accepted")
	}
}

func TestMidpoint(t *testing.T) {
	r := NewRing(4) // starts 0, 2^30, 2^31, 3*2^30
	if m, _ := Midpoint(r, 1); m != 1<<29 {
		t.Fatalf("midpoint of shard 1 = %d, want %d", m, uint32(1<<29))
	}
	// Wrapping range: sole entry owning everything starting at 100.
	wrap := Ring{Entries: []RingEntry{{Start: 100, Shard: 7}}}
	m, err := Midpoint(wrap, 7)
	if err != nil {
		t.Fatal(err)
	}
	if m != uint32((uint64(100)+(1<<31))%(1<<32)) {
		t.Fatalf("sole-shard midpoint = %d", m)
	}
	if _, _, err := Split(wrap, 7, m); err != nil {
		t.Fatalf("midpoint not splittable: %v", err)
	}
	// A range that wraps past the top: last of two entries.
	two := Ring{Entries: []RingEntry{{Start: 1 << 30, Shard: 1}, {Start: 3 << 30, Shard: 2}}}
	m, _ = Midpoint(two, 2) // width = 2^32 - 3*2^30 + 2^30 = 2^31 -> midpoint 3*2^30 + 2^30 mod 2^32 = 0
	if m != 0 {
		t.Fatalf("wrapping midpoint = %d, want 0", m)
	}
	if _, _, err := Split(two, 2, m); err != nil {
		t.Fatalf("wrapping midpoint not splittable: %v", err)
	}
	// Too narrow.
	narrow := Ring{Entries: []RingEntry{{Start: 0, Shard: 1}, {Start: 1, Shard: 2}}}
	if _, err := Midpoint(narrow, 1); err == nil {
		t.Fatal("width-1 range reported splittable")
	}
	if _, err := Midpoint(r, 99); err == nil {
		t.Fatal("unknown shard accepted")
	}
}
