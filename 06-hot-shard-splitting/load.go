package hotshard

import "sync"

// LoadTracker counts requests per shard — the raw material Day 3's hot-
// shard detector needs, and nothing more. It doesn't know about a Ring, a
// GroupServer, or an RPC boundary; whatever actually serves requests calls
// Record once it already knows which shard it's handling (it had to look
// that up to route the request anyway), and whatever's doing detection
// calls Snapshot to read the counts. Keeping it this narrow is deliberate:
// Day 1 kept Ring decoupled from group ownership for the same reason —
// "how loaded is this shard" and "who serves it" and "what range does it
// cover" are three separate questions, and tying them together here would
// make this the wrong layer to change any ONE of them in later.
//
// Not yet wired into an actual server — Stage 5's GroupServer is a
// finished, shipped type, and bolting a dynamic-shard load counter onto a
// fixed-NShards server would be the kind of change Day 4 (real splitting)
// should be making deliberately, not something that rides in quietly on a
// "just add a counter" day. This builds the mechanism and proves it works
// in isolation; wiring it into something that actually serves requests is
// later days' job, once there's a real ring-based server to wire it into.
type LoadTracker struct {
	mu     sync.Mutex
	counts map[ShardID]int
}

// NewLoadTracker returns a tracker with every count starting at zero.
func NewLoadTracker() *LoadTracker {
	return &LoadTracker{counts: make(map[ShardID]int)}
}

// Record counts one request against shard. Safe for concurrent use —
// recording real concurrent request traffic is the entire point.
func (lt *LoadTracker) Record(shard ShardID) {
	lt.mu.Lock()
	lt.counts[shard]++
	lt.mu.Unlock()
}

// Snapshot returns the counts recorded since the LAST Snapshot call (or
// since construction, for the first one) and resets every count to zero —
// "how busy has each shard been recently," not "ever." TASKS.md's own
// framing names this as the simplest thing that could distinguish "busy
// now" from "was busy an hour ago": a fixed wall-clock window would need a
// clock and a ticker goroutine just to decide when "now" starts; resetting
// on read gets the same distinction for free, as long as whatever's
// calling Snapshot does so on a roughly regular cadence (Day 3's own job).
//
// Only shards that were recorded against at least once since the last
// Snapshot appear in the result — a shard with zero traffic isn't "zero
// load," it's just absent, the same way a map with no entry for a key
// reads as its zero value either way.
func (lt *LoadTracker) Snapshot() map[ShardID]int {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	snap := lt.counts
	lt.counts = make(map[ShardID]int)
	return snap
}
