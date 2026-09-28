# Stage 5: Sharded KV Store

Package: `shardkv` (import path `github.com/aksharraikanti/distsys-lab/05-sharded-kv`)

## Why this stage

Stages 2-4 made one replicated KV store correct, reachable, and fast to read.
But it's still one Raft group: every write goes through one leader, and the
whole dataset must fit on one machine. Sharding is how a KV store gets past
that — split the keyspace, give each piece to its own replicated group.

Routing a key to the right group is the easy part. The interesting problem is
that the assignment changes — groups join, leave, load shifts — and a shard's
data has to move between groups WHILE clients keep using it. Everything hard in
this stage is a variation on one question: at every instant, exactly one group
must be answerable for each key, and that group must have every write ever
acknowledged for it, exactly once.

## Concept notes

_(fill this in as you learn — one section per day, in your own words.)_

### Day 1 — Static sharding
- Fixing the shard count and letting only the shard->group assignment change is
  the design decision everything later depends on. `Key2Shard` is a pure
  function of the key, forever; what Day 2 onward will change is who owns each
  shard. Rebalancing therefore moves whole, well-defined slices of the keyspace
  instead of rehashing keys. It's also why `Key2Shard` must be identical for
  every client and server: if two disagreed, each would write a key to a
  different group and never see the other's writes.
- FNV-1a instead of the first-byte hash MIT's lab uses, because real keys share
  prefixes (`user:`, `session:`). I checked that claim instead of asserting it:
  10,000 `user:N` keys land within ±2% of the mean on every shard, and
  swapping in a first-byte hash makes the spread test fail (every key lands on
  one shard).
- The throughput result is the honest form of sharding's promise: 270 writes/s
  with one group, 804 with three (2.97x). Latency of a single write doesn't
  improve at all — each write still pays its own group's Raft replication —
  but different groups replicate in parallel, and each group's client
  serializes only its own writes (Stage 4 Day 1's constraint, which turns out
  to be exactly the right granularity here). So the ~3ms-per-write floor from
  Stage 3 becomes a per-group floor rather than a global one.
- `NewStaticConfig`'s round-robin gives a 4/3/3 split of 10 shards over 3
  groups, and the keys-per-group log showed `map[1:30 2:17 3:13]` for 60 keys —
  more skewed than 4:3:3 because 60 keys is a small sample. Not asserted, but
  worth seeing: with only 10 shards, the granularity of balance is one whole
  shard (10% of the keyspace), which is a real limit on how evenly a few groups
  can share load and a reason production systems use many more shards than
  groups.
- Two tests taught me something about *test* design. The isolation test
  searched for "a key owned by the dead group" with an unbounded loop; under
  a broken hash (all keys to one shard) it would never find one and would hang
  for two minutes rather than fail. The whole point of that test is to catch
  routing bugs, and it converted the most likely routing bug into a hang. It
  now gives up after 5,000 tries with a message. A search inside a test needs a
  bound for the same reason a wait needs a timeout. My first "global lock"
  mutant also didn't compile (missing import) and proved nothing until redone —
  the third time this project has bitten me: a mutation result only counts if
  the mutant built and ran.
- The dead-group test shows failure isolation directly: with one group's
  endpoints crashed, every operation on the other two groups completes, a read
  of the dead group's key blocks, and when the group returns that read
  completes with its pre-outage data. Same honest limit as Stage 4: the client
  waits rather than errors, because nothing in this stack has timeouts.
- Rule of three, applied: this was the third copy of the crash-simulating
  endpoint (Stage 3, Stage 4, now here). Two copies I tolerated; the third I
  extracted into `internal/kvtest`. Migrating the two older copies onto it is
  real, unfinished work — noted rather than done, to keep this PR about
  sharding.

### Day 2 — Configurations and rebalancing (pure logic)
- `Join`, `Leave`, and `Move` are all pure functions from `Config` to `Config`
  — no I/O, no randomness, no shared state. That's not a style preference,
  it's the actual requirement: Day 3 will run these inside a Raft-replicated
  state machine, and every replica applying the same log entry has to compute
  the byte-identical result independently. A function that so much as ranged
  over a map without sorting first would silently diverge between replicas.
- The rebalance itself is the one piece of real logic: given the live group
  set, assign shards so every group holds `floor(N/g)` or `ceil(N/g)` shards
  while moving as few shards as possible off their current owner. The
  algorithm is: sort the live group ids (the only source of a stable order,
  since map iteration isn't one); the first `remainder` groups in that order
  absorb the one-shard remainder; any shard whose current owner is dead, or
  already over its new target, becomes an "orphan"; orphans get handed out,
  in ascending shard order, to groups still under target. A shard already at
  or under its group's target never moves.
- Concretely: 3 balanced groups (4/3/3 over 10 shards) plus a 4th group
  rebalances to 3/3/2/2, and exactly 2 shards move — the 2 the new group
  needs, no more. I pinned that as an exact assertion rather than just an
  upper bound, since "moves the minimum" is precise enough here to check
  precisely, and a looser bound would have let a much worse (but still
  "small") number of moves slip through unnoticed.
- Group id 0 had to mean something: `Leave` can strip every group from a
  shard (all groups left), and `Join` needs a valid starting point to build
  on (bootstrapping from nothing). I reserved 0 as "unassigned" rather than
  inventing a separate "no owner yet" type — `Validate` already rejects a
  shard pointing at a group not in `Groups`, so an all-zero `Shards` array on
  an empty `Config{}` falls out of the existing invariant for free instead of
  needing a special case.
- The property test (200 random Join/Leave sequences, 5-19 steps each, seeded
  per trial for reproducibility) checks two things after *every single step*,
  not just at the end: every shard routes to a live group, and no group holds
  more than one shard more than any other. Checking only the final state
  would miss a rebalance that transiently breaks the invariant and happens to
  self-correct by the next step — the property has to hold at every version,
  since a real controller (Day 3) serves `Query` for every historical `Num`,
  not just the latest.
- Mutation-checked two ways the algorithm could quietly go wrong: assigning
  the one-shard remainder to the *last* groups in sorted order instead of the
  first still balances correctly (every group within 1 of every other) but
  breaks determinism and moves an extra shard on Join — caught by the exact
  "moves exactly 2" assertion, not by the balance check, which is why both
  exist. Forgetting to treat a dead group's shards as orphans (just keeping
  every shard on whatever gid it already had) panics immediately with a
  slice-bounds error on the very first Join from an empty config — a loud,
  fast failure rather than a silent one, which is the outcome I actually want
  from a mutant, not a passing test I have to distrust.

_(continue per day)_

## Reference material

- MIT 6.5840 Lab 4 (Sharded Key/Value Service) — this stage's structure follows
  it, adapted to a solo daily pace, the same way Stage 2 followed Lab 3.
- Wing & Gong, "Testing and Verifying Concurrent Objects" — the linearizability
  checking algorithm Day 7's history checker is a simplified version of.

## Status

See [TASKS.md](TASKS.md) for the day-by-day checklist and [../PROGRESS.md](../PROGRESS.md) for where things stand right now.
