package hotshard

import (
	"fmt"
	shardkv "github.com/aksharraikanti/distsys-lab/05-sharded-kv"
	"math/rand"
	"testing"
)

// simCluster stands in for the data plane Stage 6 hasn't built a ring-based
// server for yet: each group owns a LoadTracker, and a request is routed by
// the CURRENT config exactly as a real one would be — config lookup picks
// the group, the group records the shard it served.
type simCluster struct {
	ck       *RingClerk
	trackers map[int]*LoadTracker
	last     map[ShardID]int // merged loads of the most recent window
	cfg      RingConfig
}

func newSimCluster(t *testing.T, ring Ring) (*simCluster, func()) {
	t.Helper()
	ctrlers, _, cleanup := newRingCtrlerCluster(3)
	ck := NewRingClerk(ctrlers)
	if err := ck.retry(func(c *RingCtrler) shardkv.CtrlerErr {
		var r InitReply
		c.Init(&InitArgs{Ring: ring, Groups: testGroups, ClientID: 999, SeqNum: 1}, &r)
		return r.Err
	}); err != nil {
		t.Fatal(err)
	}
	s := &simCluster{ck: ck, trackers: map[int]*LoadTracker{}}
	for gid := range testGroups {
		s.trackers[gid] = NewLoadTracker()
	}
	return s, cleanup
}

// run sends n requests, keys drawn by pick, routed under the live config.
func (s *simCluster) run(t *testing.T, n int, pick func() string) {
	t.Helper()
	cfg, err := s.ck.Query()
	if err != nil {
		t.Fatal(err)
	}
	s.cfg = cfg
	for i := 0; i < n; i++ {
		k := pick()
		s.trackers[cfg.Lookup(k)].Record(RingAssign(cfg.Ring, k))
	}
}

// loads is the AutoSplitter's Loads source: snapshot every group.
func (s *simCluster) loads() map[ShardID]int {
	var snaps []map[ShardID]int
	for _, tr := range s.trackers {
		snaps = append(snaps, tr.Snapshot())
	}
	s.last = MergeLoads(snaps...)
	return s.last
}

// maxGroupShare is the fraction of the last window's load carried by the
// busiest group under the config that window ran against.
func (s *simCluster) maxGroupShare() float64 {
	byGroup := map[int]int{}
	total := 0
	for sh, n := range s.last {
		byGroup[s.cfg.Owners[sh]] += n
		total += n
	}
	max := 0
	for _, n := range byGroup {
		if n > max {
			max = n
		}
	}
	return float64(max) / float64(total)
}

// skewedPicker sends hotFrac of traffic to hotKeys distinct keys that all
// hash into one shard's range, the rest uniformly across the keyspace.
func skewedPicker(ring Ring, shard ShardID, hotKeys int, hotFrac float64, rng *rand.Rand) func() string {
	var hot []string
	for i := 0; len(hot) < hotKeys; i++ {
		if k := fmt.Sprintf("hot-%d", i); RingAssign(ring, k) == shard {
			hot = append(hot, k)
		}
	}
	return func() string {
		if rng.Float64() < hotFrac {
			return hot[rng.Intn(len(hot))]
		}
		return fmt.Sprintf("cold-%d", rng.Intn(100000))
	}
}

// The Day 5 claim, end to end through a real replicated controller: one
// shard gets most of the traffic, the system splits it and moves half
// away, and the originally-hot group's load drops.
func TestAutoSplitRelievesHotGroup(t *testing.T) {
	ring := NewRing(6)
	sim, cleanup := newSimCluster(t, ring)
	defer cleanup()

	initial, _ := sim.ck.Query()
	hotShard := ShardID(2)
	hotGroup := initial.Owners[hotShard]
	pick := skewedPicker(ring, hotShard, 300, 0.8, rand.New(rand.NewSource(1)))

	as := &AutoSplitter{Ctrl: sim.ck, Policy: HotPolicy{Factor: 2, MinLoad: 100}, Loads: sim.loads, MaxShards: 16}

	// Baseline window, no Step yet: the hot group is the bottleneck.
	sim.run(t, 20000, pick)
	sim.loads() // consume the window into sim.last
	before := sim.maxGroupShare()
	if before < 0.6 {
		t.Fatalf("setup: hot group share = %.2f, expected a clearly skewed workload", before)
	}
	// Step's own Loads call needs the window back; re-run it.
	sim.run(t, 20000, pick)

	splits := 0
	for i := 0; i < 8; i++ {
		res, err := as.Step()
		if err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
		if res.Split != 0 {
			splits++
		}
		sim.run(t, 20000, pick)
	}
	sim.loads()
	after := sim.maxGroupShare()

	if splits == 0 {
		t.Fatal("skewed workload never triggered a split")
	}
	cfg, _ := sim.ck.Query()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if len(cfg.Ring.Entries) != 6+splits {
		t.Fatalf("ring has %d shards after %d splits, want %d", len(cfg.Ring.Entries), splits, 6+splits)
	}
	if after >= before-0.15 {
		t.Fatalf("hottest group's share %.2f -> %.2f; splitting did not meaningfully relieve it", before, after)
	}
	t.Logf("hot group %d: busiest-group share %.2f -> %.2f over %d splits", hotGroup, before, after, splits)
}

// A uniform workload must not trigger splits at all: relative detection
// shouldn't invent hot shards from sampling noise.
func TestAutoSplitLeavesUniformLoadAlone(t *testing.T) {
	sim, cleanup := newSimCluster(t, NewRing(6))
	defer cleanup()
	rng := rand.New(rand.NewSource(2))
	as := &AutoSplitter{Ctrl: sim.ck, Policy: HotPolicy{Factor: 2, MinLoad: 100}, Loads: sim.loads, MaxShards: 16}
	for i := 0; i < 5; i++ {
		sim.run(t, 20000, func() string { return fmt.Sprintf("k-%d", rng.Intn(1000000)) })
		res, err := as.Step()
		if err != nil {
			t.Fatal(err)
		}
		if res.Split != 0 {
			t.Fatalf("uniform load split shard %d", res.Split)
		}
	}
	if cfg, _ := sim.ck.Query(); cfg.Num != 1 {
		t.Fatalf("config advanced to %d on uniform load", cfg.Num)
	}
}

// One hot KEY can't be divided by any split — it's a single ring point.
// MaxShards must stop the splitter chasing it forever.
func TestAutoSplitMaxShardsBoundsSingleHotKey(t *testing.T) {
	sim, cleanup := newSimCluster(t, NewRing(4))
	defer cleanup()
	rng := rand.New(rand.NewSource(3))
	as := &AutoSplitter{Ctrl: sim.ck, Policy: HotPolicy{Factor: 2, MinLoad: 100}, Loads: sim.loads, MaxShards: 7}
	pick := func() string {
		if rng.Float64() < 0.9 {
			return "the-one-hot-key"
		}
		return fmt.Sprintf("k-%d", rng.Intn(1000000))
	}
	for i := 0; i < 20; i++ {
		sim.run(t, 10000, pick)
		if _, err := as.Step(); err != nil {
			t.Fatal(err)
		}
	}
	if cfg, _ := sim.ck.Query(); len(cfg.Ring.Entries) != 7 {
		t.Fatalf("ring has %d shards, want it capped at 7", len(cfg.Ring.Entries))
	}
}

// Routing safety: whatever the splitter does, every key must still resolve
// to exactly one group that exists.
func TestAutoSplitKeepsRoutingTotal(t *testing.T) {
	ring := NewRing(6)
	sim, cleanup := newSimCluster(t, ring)
	defer cleanup()
	pick := skewedPicker(ring, 1, 200, 0.8, rand.New(rand.NewSource(4)))
	as := &AutoSplitter{Ctrl: sim.ck, Policy: HotPolicy{Factor: 2, MinLoad: 100}, Loads: sim.loads, MaxShards: 16}
	for i := 0; i < 6; i++ {
		sim.run(t, 10000, pick)
		if _, err := as.Step(); err != nil {
			t.Fatal(err)
		}
	}
	cfg, _ := sim.ck.Query()
	for i := 0; i < 5000; i++ {
		k := fmt.Sprintf("probe-%d", i)
		if _, ok := cfg.Groups[cfg.Lookup(k)]; !ok {
			t.Fatalf("key %s routes to nonexistent group %d", k, cfg.Lookup(k))
		}
	}
}
