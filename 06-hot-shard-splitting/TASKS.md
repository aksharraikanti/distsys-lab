# Stage 6 Tasks: Hot Shard Detection and Splitting

Builds directly on Stage 5: shards are currently FIXED in number and FIXED in
size — `NShards` is a constant, and `Key2Shard` maps every key to one of a
fixed set of hash buckets. Stage 5's whole rebalancing story (`Join`/`Leave`/
`Move`, migration, GC) can move a shard from one group to another, but it can
never make a shard SMALLER. A single hot key, or a cluster of related keys
that all hash near each other, can pin one group as the system's bottleneck
no matter how many groups exist — adding groups only helps if the hot shard's
own load can actually be split across more than one of them. This stage adds
the one capability Stage 5 is missing: splitting a shard that's carrying more
than its share of load into two, each independently assignable afterward.

Package: `hotshard` (import path
`github.com/aksharraikanti/distsys-lab/06-hot-shard-splitting`), building on
`05-sharded-kv` directly — a shard group here IS a Stage 5 `GroupServer`, and
a split, once decided, is proposed through the same shard controller
(`Ctrler`) and migrates through the same `migrationLoop`/`gcLoop` Stage 5
already built. Nothing about REPLICATING a shard's data changes in this
stage; what changes is how many shards there ARE and how that number grows
in response to what the system is actually seeing, not a number picked in
advance.

The problem, concretely: Stage 5's `Key2Shard(key) = hash(key) % NShards` is
a function of `NShards` alone — splitting shard 3 into two would change what
EVERY other key's shard number means too, unless shard identity stops being
"a number out of NShards" and becomes something that can subdivide on its
own. Consistent hashing (keys and shards both placed as points/ranges on one
hash ring) is the standard answer: a shard is a RANGE on the ring, splitting
one range into two touches only that range, and every key outside it keeps
mapping to exactly the shard it already did.

Scope: detection and splitting only. This stage does not implement merging
two cold shards back together (a real system would want it, symmetrically,
but TASKS.md keeps this stage to the half of the problem its name promises)
and does not change anything about single-key correctness — Stage 5's
migration/dedup/GC machinery is reused as-is, not re-derived, because
splitting a shard and reassigning a shard are the same kind of event
(a configuration change that moves data) once the ring makes "shard" a range
instead of a fixed bucket.

- [x] **Day 1 — Consistent hashing ring (pure logic).** Replace `Key2Shard`'s
      fixed `hash(key) % NShards` with a ring: a shard is identified by a
      `[Start, End)` range of hash space, not a bucket number, and a `Ring`
      type (replacing `Config.Shards [NShards]int`) maps ranges to group ids.
      `RingAssign(ring, key) -> shardID` is a pure function. Property tests:
      every key maps to exactly one range, ranges partition the WHOLE ring
      with no gaps and no overlaps, and splitting a range (Day 4) never
      changes which shard any key OUTSIDE that range belongs to — the
      property Stage 5's fixed `NShards` scheme couldn't offer.
      Built as `ShardID` (a stable identity surviving a future Move, the way
      Stage 5's shard index never could once shards can be created) +
      `RingEntry{Start, Shard}` + `Ring{Entries []RingEntry}`, kept sorted
      ascending by Start so `RingAssign` is a binary search — "ring," not
      "line," because a hash BELOW every stored Start wraps to the entry
      with the HIGHEST Start, the same stretch of space it already owns.
      `Split` is self-validating (no replicated boundary built yet to do
      that for it, unlike Stage 5's Join/Leave/Move) and property-verified:
      a key outside the split shard's range keeps its exact owner, one
      inside lands on whichever of the two new halves actually contains it,
      never anything else. Didn't carry ownership (shard -> group) in `Ring`
      at all — that's a deliberately separate concern, reused from Stage 5's
      own Config once a later day wires this into something replicated.
      Mutation testing found two real gaps: `inRangeExclusive`'s wrap branch
      removed broke the boundary-partition property test immediately, but
      `RingAssign`'s own wrap fallback (`Entries[n-1]` vs `Entries[0]`) was
      UNREACHABLE through every test that built rings via `NewRing`, since
      `NewRing` always starts its first entry at exactly 0 — no key's hash
      can ever be "below" that. Refactored `RingAssign` to delegate to an
      internal `ringAssignPoint`, testable against hand-picked boundary
      values a hand-built (non-`NewRing`) ring can actually exercise.
- [x] **Day 2 — Per-shard load tracking.** Each `GroupServer` counts
      requests per shard it owns (a lightweight counter, reset on read — no
      need for anything fancier than Stage 4's own `Stats` pattern), exposed
      over a new RPC so something outside the group can ask "how hot is
      shard X right now." Decide the counting window (a fixed wall-clock
      interval, reset each time it's read, is the simplest thing that could
      distinguish "busy now" from "was busy an hour ago").
      Scoped narrower than the bullet first promised: built `LoadTracker` —
      per-`ShardID` counters, `Record`/`Snapshot` (reset-on-read, the
      window choice named above) — as standalone, server-agnostic
      machinery, verified in isolation (per-shard isolation, reset
      semantics, concurrent `Record` safety). Did NOT wire it into Stage
      5's `GroupServer` or add an RPC today: that server is a finished,
      shipped type built around a fixed `NShards`, and bolting a
      dynamic-shard counter onto it would be the kind of structural
      decision Day 4 (real splitting) should make on purpose, not something
      that rides in on a "just add a counter" day before a ring-based
      server even exists to own it. Mutation-checked: `Snapshot` not
      actually resetting its counts was caught immediately by the
      reset-semantics test.
- [x] **Day 3 — Hot shard detection.** A monitor (polling every group the
      way `ShardClerk` already knows how to reach them) aggregates load
      reports and flags a shard "hot" by a threshold relative to the mean
      across all shards — not an absolute number, since "hot" only means
      anything relative to how loaded everything else currently is. Pure
      decision logic first, tested against synthetic load reports before
      it ever touches a real cluster.
      Built `DetectHot(loads, shards, HotPolicy)` + `MergeLoads` as pure
      functions, no cluster or polling loop yet (that's Day 5's wiring).
      A shard is hot when its load strictly exceeds `Factor` x the mean
      over ALL shards in the ring — idle shards absent from a `Snapshot`
      count as zero in the mean, or one reporting shard would be its own
      mean and never look hot. Optional `MinLoad` floor keeps near-idle
      clusters from flagging noise. Result is hottest-first, ties by
      ShardID. Mutation-checked: `>=` for `>` and mean-over-reported-only
      were both caught.

- [ ] **Day 4 — Splitting a shard (replicated).** A `Split` operation on the
      ring: pick a hot shard's range, choose a split point (the range's
      midpoint to start; a load-weighted point is future work, not this
      day's), and replace the one range with two, BOTH initially assigned to
      the shard's CURRENT group — no data movement at split time, mirroring
      Stage 5 Day 2's own "assignment first, migration follows" philosophy.
      Proposed through `Ctrler` as a new config version, same as Stage 5's
      `Join`/`Leave`/`Move`.
- [ ] **Day 5 — Automatic splitting end-to-end.** Wire Day 3's detector to
      actually propose Day 4's `Split` once a shard crosses the hot
      threshold, then — since the newly split shard is still on the same
      group — a normal `Move` (Stage 5, unchanged) relocates it to relieve
      the group that was hot. Prove it with a synthetically skewed key
      distribution: one shard gets most of the traffic, the system splits
      it and moves half away, and the originally-hot group's load drops.
- [ ] **Day 6 — Full integration under a real skewed workload.** A Zipfian
      load test (Stage 4's own precedent) against a multi-group cluster with
      detection and auto-splitting both live, measuring whether throughput
      for the hot keys actually improves after a real split — not just that
      a split happened, but that it was worth doing. Fault injection and
      concurrent clients layered on top, same as Stage 5 Day 8, judged by
      Stage 5's own `IsLinearizable` checker (reused directly — splitting
      changes which shard a key lives in, not the single-key register
      semantics the checker verifies).

## Done means

- `go test ./06-hot-shard-splitting/... -race` passes, including the Day 6
  integration suite.
- Stage 6 README's concept notes are filled in for every day.
- `PROGRESS.md` updated, Stage 7 scoped next.
