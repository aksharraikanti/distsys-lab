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

### Day 3 — The shard controller
- The honest test this day sets up ("is Stage 2's machinery really reusable,
  or was it quietly specific to a string map?") came back positive with one
  real wrinkle. `applyLoop`, the dedup table, notify channels, the leader-
  check ticker, and no-op-on-election all carried over with the state
  swapped from a map write to `c.configs = append(c.configs,
  Join(latest, ...))` — Day 2's pure functions being the actual mutation
  logic is exactly why this dropped in cleanly. The one thing that
  couldn't carry over unchanged: `PutAppend`'s supersession check compares
  the whole applied `Op` (`applied != op`), which only compiles because
  every field of `kvstore.Op` is comparable. `ctrlerOp` isn't — it carries
  `JoinGroups map[int][]string` and `LeaveGIDs []int` — so the check became
  `applied.ClientID != op.ClientID || applied.SeqNum != op.SeqNum` instead.
  Once I looked at it, that's arguably the more correct statement of what
  the check actually means (identity of the request), not a workaround.
- `Query` reuses `Get`'s exact Raft §8 gate (leader AND own-term no-op
  applied) rather than skipping it because "it's just a read." A leader that
  answered `Query` before its own no-op applied could omit a config version
  the previous leader had already acknowledged to a client — the identical
  failure mode Stage 3's fault test found in `Get`, just for config history
  instead of key-value data.
- Mutation-checked three ways, and the third one found a real hole. Removing
  the dedup guard entirely: caught immediately (a retried Join appended a
  second config version, `TestCtrlerRetriedJoinAppliesOnlyOnce` failed).
  Removing the Raft §8 gate from `Query`: **nothing in the existing suite
  noticed** — every test either drives a single settled leader or explicitly
  waits for one, so the exact window the gate exists to close was never
  actually exercised. That's the same class of gap this project has hit
  before (a test that passes for reasons unrelated to what it's supposed to
  check), so I built a dedicated whitebox test instead of trusting the
  existing coverage: construct a `Ctrler` by hand around a node forced
  straight to Leader (`BecomeCandidate`/`BecomeLeader`, `noopLoop` never
  started), assert `Query` refuses, then propose the no-op myself and assert
  it succeeds once applied. That test fails reliably against the mutant and
  passes against the real gate.
- `Move` validates against a snapshot of the current config taken right
  before proposing, not at apply time — a group could theoretically `Leave`
  in the gap between that check and the entry committing, landing a `Move`
  on a group that's already gone. Flagged in `CtrlerErrInvalidArgs`'s own
  doc comment as a known, accepted gap: closing it needs the apply-time
  check to report "committed but rejected" back through the notify channel,
  which is more machinery than a day about proving the replicated log itself
  works calls for.
- No snapshotting. `KVServer` didn't get it until Stage 2's Day 6, and a
  config history — one entry per `Join`/`Leave`/`Move`, not per key written
  — is orders of magnitude smaller than a KV store's state in any run this
  project will actually do. If that stops being true, the same
  `kvSnapshot`/`RaftStateSize` pattern carries over directly; it just isn't
  earned yet.
- `CtrlerClerk` talks to `*Ctrler` in-process, the same way `Clerk` talked to
  `KVServer` before Stage 3 existed. Day 4 ("groups poll the controller") is
  where a real net/rpc face (mirroring `ServeKVServer`) actually becomes
  necessary — adding it here would be building ahead of what this day proves.

### Day 4 — Groups serve only their shards
- Revised Day 3's own prediction: a real net/rpc face for `Ctrler` turned out
  to NOT be necessary yet either. `GroupServer` polls the controller
  in-process too, through its own `*CtrlerClerk` — proving the ownership
  mechanism doesn't need real sockets between processes, only real Raft logs
  within each one. Real networking is still coming; it's just further out
  than I expected two days ago, most likely once migration (Day 5) needs
  groups to actually pull data from each other across process boundaries.
- The concrete answer to "what does 'through the log, not a local variable'
  mean in code": `GroupServer.cfg` is written in exactly ONE place —
  `applyLoop`, from a committed `Config` entry — and every ownership
  decision (`Get`'s gate, `PutAppend`'s apply-time check) reads that same
  field under the same lock. Every replica applies the identical sequence of
  `Config` and `Put`/`Append` entries in the identical order, so there is no
  point where two replicas — or this replica across its own election — could
  disagree about which config was current for a SPECIFIC entry. A version
  living in a variable `configPollLoop` wrote directly, instead, would break
  that: followers wouldn't have it at all until their own poll tick, and a
  freshly elected leader could momentarily believe whatever its predecessor's
  poll loop last wrote, not what the cluster actually committed.
- `Get` reuses `KVServer`'s and `Ctrler`'s identical Raft §8 gate
  (leader + own-term no-op applied) before trusting `cfg` for anything —
  "read through the log" was the answer to TASKS.md's own open question,
  because the machinery already existed and a lease would have been new
  machinery for the same guarantee.
- One real design choice: `PutAppend` has TWO ownership checks, not one. The
  pre-check (before proposing) is pure optimization — it keeps an
  obviously-wrong-group write out of the Raft log entirely. The apply-time
  check (inside `applyLoop`, against `cfg` as of that specific committed
  index) is the actual authority. I didn't understand how separate these two
  really were until mutation testing showed it: turning the apply-time check
  into `if false` passed every single test in my first draft, because every
  test routed writes through `PutAppend`, and the pre-check caught the
  obvious cases before the mutant ever got a chance to matter. Had to write
  a test that bypasses `PutAppend` entirely and proposes a Config directly
  ahead of a Put on the same leader — reproducing "ownership revoked in the
  gap between check and commit" deterministically, with no election timing
  needed, since a single leader's own Proposes land in the log in the exact
  order it calls them.
- Second mutation gap, same session: `Config.Num == cfg.Num+1` relaxed to
  `Config.Num > cfg.Num` also passed every existing test, because
  `configPollLoop`'s own discipline (always request exactly `cfg.Num+1`,
  never the latest) means the production code never actually generates a
  version-skipping proposal — so nothing exercised the difference. It
  matters anyway, for Day 5: a group that jumped straight from config 3 to
  config 5 would never see config 4's transition at all, and migration has
  to react to EVERY transition, not just wherever things end up. Pinned with
  a direct test proposing Num 1 then Num 3 and checking the skip is rejected.
- Both gaps share a shape worth naming: a fast-path optimization (the
  pre-check) or an invariant the CALLER already upholds (the poll loop's own
  discipline) can fully hide a missing check in the code path that's
  supposed to be the real guarantee. The lesson isn't "test more" in
  general, it's "test the authority, not just the common path that happens
  to agree with it."
- The known gap Stage 2's `Get` already carries is inherited here unchanged:
  a leader silently partitioned away keeps answering from its own last-known
  `cfg` — it doesn't know a newer config, reassigning its shards elsewhere,
  has since committed on the other side of the partition. Closing that needs
  a lease or read-index scheme, which TASKS.md itself offered as an
  alternative to "read through the log" — flagged, not fixed, matching how
  this exact gap has been left open (and re-flagged) at every earlier stage
  that hit it.

### Day 5 — Shard migration
- The dedup table travels with the shard WHOLE, not filtered down to just
  that shard's clients. It's tempting to want to scope it — "only send the
  dedup entries for clients who've touched THIS shard" — but a client's
  SeqNum is a single strictly-increasing counter across every key it ever
  writes, regardless of shard, so there's no clean subset to compute without
  tracking per-shard-per-client history nobody else needs. Sending the whole
  table and merging by MAX per ClientID is safe (a client's dedup fact is
  either irrelevant to the recipient's OWN shards, in which case it just sits
  there unused, or relevant, in which case it's exactly right) and is what
  real solutions to this exact problem do.
- `migrationLoop` mirrors `configPollLoop`'s whole shape on purpose: leader-
  only, fire-and-forget Propose, a local "already proposed, don't re-log
  every tick" cache reset on losing leadership. The thing being proposed
  (`Migrate`) doesn't even need dedup the way Put/Append do — merging the
  same data or dedup entries twice is a no-op, so a lost-then-retried
  proposal is free to just happen again. That asymmetry between "needs exact
  ClientID/SeqNum dedup" (Put/Append) and "naturally idempotent, retry away"
  (Config, Migrate) is really about whether the OPERATION has externally
  observable side effects beyond its own state (a client waiting on a
  specific reply) or not.
- `Pull` doesn't require the donor to be leader, and answering from a
  follower is genuinely safe, not just convenient: once a replica's own
  applied config has passed the point where it stopped owning a shard, Day
  4's own apply-time check already guarantees NOTHING can mutate that
  shard's data on this replica ever again — the data is frozen, and every
  replica reaches the identical frozen state via the identical log, whether
  it happens to be leader or not. The harder question was a race I didn't
  see until I sat down to write `Pull`'s docs: could a NEW owner start
  pulling from a donor that hasn't yet itself caught up to the config
  transition, catching some writes but not others still in flight? Yes —
  which is exactly why `Pull` gates on `donor.cfg.Num >= args.ConfigNum`,
  not on "donor currently believes it owns nothing here."
- A second gate I only found by asking "what if the SAME shard gets
  reassigned again before its first migration even lands?": B pulls shard S
  from A, then — before B has actually finished that pull — the controller
  reassigns S again, this time to C. If C asked B for the shard the instant
  B's OWN config caught up to the reassignment, B would hand over whatever
  it has, which might be nothing at all. `Pull` also refuses
  (`GroupErrNotReady`) if the DONOR itself still has that shard in its own
  `migrating` map. This is deliberately a safety fix, not a liveness one: it
  stops corruption, but if B's own pull from A stalls forever, so does C's
  from B. Rapid reassignment of a shard before earlier migrations settle is
  explicitly not this day's problem to solve for real — TASKS.md's own Day 6
  ("concurrent clients through reconfiguration") is where that kind of
  pressure is meant to show up and get handled.
- Mutation testing again found a gap invisible to my first pass, and it
  rhymes with Day 4's: the migrating-gate itself (skip data migration
  entirely) broke a whole test immediately, since every test writes data
  before moving it and checks it's still there. But the dedup merge's `if
  seq > existing` collapsed to a plain overwrite and passed EVERYTHING —
  because every migration in every test landed a ClientID the recipient had
  never independently seen before, so "take the max" and "just overwrite"
  produced identical results every time. Only a test that pre-seeds the
  RECIPIENT with a higher SeqNum before migrating in a stale, lower one for
  the same client actually distinguishes the two. Same shape as Day 4's
  gaps, again: the common path in every test I'd already written happened to
  agree with a weaker, wrong implementation.
- Still no garbage collection: the old owner of a migrated shard keeps its
  copy forever, unused, unreachable through its own `Get`/`PutAppend` but
  still sitting in `store`. TASKS.md names this explicitly as Day 6's own
  "challenge" scope, not an oversight here — every migration currently
  leaks a shard's worth of memory on whoever used to hold it.

### Day 6 — Concurrent clients through reconfiguration
- Garbage collection is Day 5's own `migrating` field mirrored: `leaving`
  tracks shards THIS group gave away, populated at the exact same moment
  (and by the exact same Config-apply code) as `migrating` is — one loop
  over all `NShards` now checks both directions (gained from someone real,
  lost to someone real) instead of just one. Once a design has "the log is
  the only writer of this state" as a working pattern, the second use of it
  is mostly just recognizing the shape again, not inventing something new.
- The donor doesn't just trust that a config transition happened — it asks
  the recipient directly (`HasShard`) whether it's actually ready, and only
  then Proposes the deletion to its OWN log. That asking-first structure is
  what makes this safe rather than merely probable: without it, GC would be
  racing migration with nothing enforcing who wins.
- `HasShard` needed no new machinery at all — it's `ownsLocked`'s own check
  (cfg says so, and not still migrating), read out through an RPC instead of
  used internally. Worth noticing when a new public interface is really
  just exposing something a private one already computes.
- I originally reasoned "gcLoop always asks the CURRENT `leaving` entry, so
  it naturally follows a shard through a chain of reassignments" — wrong,
  and I caught it by actually tracing what happens to `leaving[shard]` when
  a THIRD group takes the shard from the second before the donor's asked
  anyone anything. `leaving[shard]` is written only by THIS group's own
  Config-apply step, which stops firing for that shard the instant this
  group gives it away — so it keeps the ORIGINAL recipient's identity
  forever, never learns the shard moved again. Safe (this group just keeps
  an orphaned copy rather than corrupting anything), not fully live for a
  fast reassignment chain — an explicit, accepted scope boundary, not a bug
  I'm pretending isn't there.
- Mutation-checked the GC deletion itself (caught immediately — the
  dedicated GC test fails within its 3s wait, since nothing ever clears the
  old owner's copy). The safety GATE (`recipientReady`) was a different
  story: mutating it to unconditionally return true was only caught by the
  full end-to-end GC test on SOME runs, not every one — the actual race
  between "migration lands" and "GC fires early" depends on real goroutine
  scheduling, so a test built on real timing can't promise to observe it
  every time. Wrote a direct, deterministic unit test of `recipientReady`
  instead (three hand-built recipients: not-yet-owner, still-migrating,
  actually-ready), which fails every single time the gate is broken. Same
  root lesson as Day 4's "test the authority, not the common path" —
  extended: when the authority's failure mode is itself timing-dependent,
  the deterministic version of that lesson is to unit-test the gate in
  isolation, not just lean harder on the integration test.
- The concurrent stress test is Stage 2 Day 5's private-key trick again,
  now with the ground moving: 5 clients Appending to their own keys while
  the cluster grows from 1 group to 3, two shards Move explicitly, and the
  original group Leaves — all overlapping real client traffic, not run
  before or after it. `ShardClerk`'s retry-on-`GroupErrWrongGroup` is
  doing real work here: every client keeps writing successfully straight
  through several shards changing owners underneath it, without ever
  knowing a migration happened. The one bug this surfaced was in the TEST,
  not production: sharing a single `*CtrlerClerk` across the concurrent
  client goroutines raced (it's explicitly documented as not safe for
  concurrent use, same as `ShardClerk` itself) — each simulated client
  needed its own, exactly the one-Clerk-per-actor rule this project has
  followed since Stage 2's own `Clerk`.
- No timeouts anywhere in this stack still means what it's meant since
  Stage 2: `ShardClerk` waits, it doesn't give up. The stress test's 30s
  outer bound is a test-harness safety net against a real bug hanging
  forever, not a production timeout — the actual runs finish in ~150ms.

_(continue per day)_

## Reference material

- MIT 6.5840 Lab 4 (Sharded Key/Value Service) — this stage's structure follows
  it, adapted to a solo daily pace, the same way Stage 2 followed Lab 3.
- Wing & Gong, "Testing and Verifying Concurrent Objects" — the linearizability
  checking algorithm Day 7's history checker is a simplified version of.

## Status

See [TASKS.md](TASKS.md) for the day-by-day checklist and [../PROGRESS.md](../PROGRESS.md) for where things stand right now.
