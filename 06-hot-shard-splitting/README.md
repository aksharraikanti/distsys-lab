# Stage 6: Hot Shard Detection and Splitting

Package: `hotshard` (import path `github.com/aksharraikanti/distsys-lab/06-hot-shard-splitting`)

## Why this stage

Stage 5 made sharding work: a fixed number of shards, each assigned to a
group, rebalanced by moving whole shards around when groups join or leave.
That's enough as long as the keyspace is loaded roughly evenly — but real
workloads aren't. A single popular key, or a run of keys that happen to hash
near each other, can pin one group as the whole system's bottleneck no
matter how many other groups exist, because Stage 5 can move a shard but can
never make one SMALLER. This stage is about noticing when that's happening
and actually doing something about it: splitting an overloaded shard into
two, each independently assignable, so the groups that were idle can finally
take some of the load.

## Concept notes

_(fill this in as you learn — one section per day, in your own words.)_

### Day 1 — Consistent hashing ring
- The actual design problem Day 1 had to solve wasn't "implement a ring" —
  it was "figure out what a shard's IDENTITY even means once shards aren't
  a fixed array anymore." Stage 5's shard WAS its index into `Shards
  [NShards]int`; that's free identity, as long as the array never changes
  shape. The moment a shard can be born from a split, something has to give
  it an identity that doesn't depend on its position — `ShardID`, assigned
  once at creation and never reused, is that something. Everything else in
  the design (the ring, the binary search, the wrap) falls out of needing
  `RingAssign` to answer "which ShardID owns this key" without caring how
  that ShardID came to exist.
- Deliberately did NOT fold group ownership into `Ring`. It would have been
  easy to make `RingEntry{Start, GroupID}` directly, mirroring Stage 5's
  `Config.Shards[s] = gid` shape — but then a `Move` (reassigning a shard to
  a different group, unchanged from Stage 5) and a `Split` (changing how
  many shards exist) would both be mutations of the SAME structure, for two
  reasons that have nothing to do with each other. Keeping `Ring` purely
  about "how many shards, what range does each own" and treating "which
  group serves which shard" as a separate concern (reusing Stage 5's own
  answer once this gets wired into something replicated) keeps each
  question answerable independently — which is exactly what Day 2's load
  tracking needs: load is a property of a SHARD, tracked by `ShardID`,
  regardless of which group happens to be serving it at the time.
- The property tests TASKS.md asked for turned out to need two different
  kinds of input to actually mean something. "No gaps or overlaps" needed
  RAW ring positions (including exact boundary points and the point just
  before each one) — hashing random string keys would almost never land
  exactly ON a boundary, so a bug narrowly confined to boundary handling
  could hide behind thousands of passing interior-point checks. "Splitting
  doesn't change ownership outside the split range" needed the opposite —
  real hashed keys, lots of them, specifically to prove the property holds
  under REALISTIC key distribution, not just at a few hand-picked points.
- Mutation testing found a genuinely interesting kind of gap: `RingAssign`'s
  own wrap-around fallback (what happens when a hash falls before every
  stored Start) was completely unreachable through my entire test suite,
  not because I forgot to test it, but because `NewRing` always starts its
  first entry at exactly 0 — there is no uint32 value "before" 0. Every
  single test that built a ring through `NewRing` was structurally
  incapable of exercising that branch, no matter how many random keys it
  tried. The fix wasn't a better test of the SAME shape, it was recognizing
  that only a hand-built `Ring` (skipping `NewRing` entirely, deliberately
  starting its lowest entry somewhere other than 0) could ever reach that
  code path — refactored `RingAssign` to delegate to an internal,
  point-based `ringAssignPoint` specifically so a test could hand it exact
  boundary values instead of hoping a hashed key would wrap there by luck.
  Same root lesson Stage 5 kept relearning (test the actual authority, not
  whatever path the common construction happens to take) in a new shape:
  sometimes the "common path" isn't just the likely one, it's the ONLY one
  your own constructors can ever produce.

### Day 2 — Per-shard load tracking
- The real decision this day was scoping it SMALLER than the TASKS.md
  bullet literally asked for, not building what it asked for. "Each
  `GroupServer` counts requests" names a specific, already-shipped Stage 5
  type — but `GroupServer` was built around a fixed `NShards` array, has no
  idea what a `Ring` or a `ShardID` is, and changing that is exactly the
  kind of structural decision Day 4 (actually wiring splitting in) should
  make on purpose, with the full picture of what's being built, not
  something that rides in quietly on a day whose own headline is "add a
  counter." Building `LoadTracker` as standalone, server-agnostic
  machinery — then proving it correct entirely on its own — means Day 4
  inherits a piece that already works, instead of inheriting a
  half-considered change bolted onto a server that was never designed for
  it.
- Reset-on-read turned out to be a genuinely good fit, not just the
  "simplest thing that could work" TASKS.md named it as. It needs no clock,
  no ticker goroutine, no separate "when did this window start" state — the
  window IS whatever time elapsed between two `Snapshot` calls, decided
  entirely by whoever's calling it. The cost is that the caller's own
  POLLING cadence becomes the comparison's granularity; Day 3's detector
  gets to decide how often "recently" means, which is actually the right
  place for that decision to live, not baked into the tracker itself.
- The one genuinely interesting design question was almost invisible:
  should `Snapshot()` return a DEFENSIVE COPY of the counts, or can it hand
  back the live map directly? Handing back the live map is only safe
  because the very next line replaces `lt.counts` with a brand new map
  under the same lock — by the time `Snapshot` returns, nothing inside the
  tracker holds a reference to the map it just gave away anymore. It's a
  full ownership transfer, not a shared view, so there's nothing left to
  race on. Worth noticing because the EASY version of this code (copy
  defensively, always) would have worked too, just done strictly more
  copying for no actual safety gained.
- Mutation testing caught the one thing actually worth checking here:
  deleting the reset (`Snapshot` returning without replacing `lt.counts`)
  broke the reset-semantics test immediately — a second `Snapshot` with no
  new traffic in between would otherwise just keep re-reporting the same
  old counts forever, silently turning "how busy right now" into "how busy
  ever," which is a correctness bug a hot-shard detector built on top of it
  would never be able to tell apart from a genuinely still-hot shard.

_(continue per day)_

## Reference material

- Dynamo-style consistent hashing (Karger et al., "Consistent Hashing and
  Random Trees") — the ring model this stage's shard ranges are built on.
- Bigtable / HBase / CockroachDB's own range-splitting — real systems that
  solve exactly this problem, for the same reason: a fixed partitioning
  scheme can't track a workload that changes after the system was designed.

## Status

See [TASKS.md](TASKS.md) for the day-by-day checklist and [../PROGRESS.md](../PROGRESS.md) for where things stand right now.
