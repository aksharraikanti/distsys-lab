# Stage 5 Tasks: Sharded KV Store

Builds on everything before it: each shard GROUP is a Stage 2 Raft-replicated
KV cluster, clients reach groups over Stage 3's pooled connections, and Stage
4's cache can sit in front of the whole thing. Structure follows MIT 6.5840
Lab 4 (shardctrler + shardkv), adapted to this repo's from-scratch, day-a-time
pace — the same relationship Stages 1 and 2 have to Labs 2 and 3.

Package: `shardkv` (import path `github.com/aksharraikanti/distsys-lab/05-sharded-kv`).

The problem: one Raft group holds every key, so its capacity is one machine's,
and every write funnels through one leader. Sharding splits the keyspace into
a fixed number of SHARDS and assigns each shard to a GROUP; a CONFIGURATION is
the versioned shard->group map. The hard part isn't routing, it's changing the
configuration while clients keep reading and writing: a shard's data (and its
dedup table) must move from one group to another without a write being lost,
applied twice, or served by a group that no longer owns it.

Scope: single-key operations only. Multi-key atomicity across shards is
Stage 7 (distributed transactions); this stage is about keeping ONE key
correct while its shard moves.

- [x] **Day 1 — Static sharding.** A fixed shard count (`NShards`, e.g. 10), a
      `key2shard` function, several independent Stage 2 clusters as groups,
      and a `ShardedClient` that routes each key to the group owning its
      shard under a config supplied at construction — no controller, no
      migration yet. Prove keys spread across groups, that the same key
      always routes to the same group, and that a single-group failure only
      affects the shards that group owns. Benchmark write throughput with 1
      vs 3 groups: sharding's whole promise is that writes scale, and it's
      worth measuring whether they do (Raft replication latency per group is
      the floor — throughput should scale, latency shouldn't drop).
      Measured: 1 group 270 writes/s, 3 groups 804 writes/s (2.97x; ideal 3x).
      Building the fault-injectable multi-cluster test setup made this the
      third copy of Stage 3/4's `crashableEndpoint`, so it was extracted into
      `internal/kvtest` (the older two copies are not yet migrated onto it).
- [x] **Day 2 — Configurations and rebalancing (pure logic).** The `Config`
      type (`Num`, `Shards [NShards]int` group ids, `Groups map[int][]string`)
      and `Join`/`Leave`/`Move` as PURE functions from config to config. The
      rebalance must be deterministic (every replica of the controller will run
      it and must get the identical answer — Go map iteration order is random,
      so this needs care) and must move the MINIMUM number of shards. Property
      tests over random join/leave sequences: every shard always assigned to a
      live group, load balanced to within 1 shard, minimal movement.
      Verified: 3 balanced groups (4/3/3) + a 4th rebalances to 3/3/2/2 moving
      exactly 2 shards (the minimum). 200 random Join/Leave sequences checked
      for both invariants after every step, not just the final state. Group id
      0 reserved as "unassigned" so `Leave`-ing every group and bootstrapping
      `Join` from `Config{}` both fall out of `Validate`'s existing checks with
      no special case.
- [x] **Day 3 — The shard controller.** Make the config history a replicated
      service: a Raft-backed state machine (reusing Stage 2's apply-loop,
      dedup, and leader-change machinery) with `Join`/`Leave`/`Move`/`Query`.
      `Query(num)` returns an old config, `-1` the latest. Clerk-style client
      with retries. This is Stage 2 again in a new costume, and the test is
      whether the machinery really was reusable or whether Stage 2 was
      quietly specific to a string map.
      Verified: the machinery carried over unchanged (apply-loop, dedup
      table, notify channels, no-op-on-election, leader-check ticker) — only
      the state mutated (append a `Config` computed by Day 2's pure
      `Join`/`Leave`/`Move`, instead of a map write) and the same-op-equality
      check (not comparable: `ctrlerOp` carries a map and a slice — compared
      by `ClientID`+`SeqNum` instead). In-process `CtrlerClerk` only (real
      RPC serving is Day 4's job, once groups actually need to poll it).
      Retried Join/Leave/Move dedups to one config version; a leader cutoff
      mid-Join converges via commit-before-cutoff or the client's retry
      against the new leader, never zero or two new versions. A mutation
      check caught a real gap in my own first test pass: removing `Query`'s
      Raft §8 no-op gate broke nothing in the existing suite, so a dedicated
      whitebox test (built by hand, bypassing `noopLoop`'s ticker) was added
      to actually exercise the window it's supposed to close.
- [x] **Day 4 — Groups serve only their shards.** Each group polls the
      controller for the current config and serves a key only if its shard is
      assigned to it, otherwise `ErrWrongGroup` (the client refetches the
      config and retries). Ownership must be checked through the group's Raft
      log, not a local variable: a leader can lose a shard while a stale
      belief lingers — the same hazard as Stage 2's stale-leader `Get` gap,
      except here it can hand out a shard's data after the shard has moved.
      Decide how reads handle it (read through the log, or a lease).
      Built `GroupServer`, a new type (like `Ctrler`, not a KVServer
      subclass): a leader polls the controller for exactly `cfg.Num+1`
      (never the absolute latest — configs must be adopted one at a time,
      for Day 5's sake) and Proposes it as a `Config` log entry; every
      Put/Append is checked against `cfg` at the moment IT applies, not
      whenever it was proposed or pre-checked — that's what "through the
      log" means concretely. Chose "read through the log" over a lease:
      `Get` reuses the exact Raft §8 no-op gate `Ctrler.Query`/`KVServer.Get`
      already use. Mutation testing found TWO real gaps invisible to the
      first test pass: `PutAppend`'s own pre-check (a fast-path rejection)
      masked the apply-time check in every test that used it, so a
      dedicated test proposes a Config directly ahead of a Put to force the
      exact race the pre-check can't close; and `Config.Num == cfg.Num+1`
      (not just `>`) was unverified since the poll loop's own discipline
      never triggers the difference — a version-skip test now pins it,
      since Day 5's migration needs every transition, not just the final
      state. The known partition gap Stage 2's `Get` already carries (a
      silently-partitioned leader keeps answering from its own stale view)
      applies here too, unresolved, same as there.
- [x] **Day 5 — Shard migration.** When a group's config changes, the new owner
      PULLS the shards it gained from their previous owner, and the dedup
      table moves WITH the shard (otherwise a client retry that spans a
      migration double-applies — Stage 2 Day 3's bug, reintroduced). Config
      changes are themselves log entries, so every replica in a group agrees
      on exactly when a shard changed hands. Operations on a shard mid-move
      must wait or fail with `ErrWrongGroup`, never be lost or served stale.
      Adopting a Config that grants a REAL shard (previous owner != 0, != me)
      marks it `migrating` instead of ready; `migrationLoop` (leader-only)
      Pulls it and Proposes a `Migrate` entry once data actually arrives —
      landing through the log too, so every replica gets the same data at
      the same point, not just the leader that happened to fetch it. `Pull`
      only answers once the donor's OWN applied config has caught up to the
      transition (its data is provably frozen by then) AND the donor isn't
      itself still migrating that shard in — the second check matters for a
      shard reassigned again before its first migration finishes; without
      it a donor could hand off data it doesn't actually have complete yet.
      Whole `duplicateTable` travels with every migration (not scoped per
      shard — a client's SeqNum sequence isn't shard-scoped either), merged
      by MAX per ClientID, never overwritten. Mutation testing found two
      real gaps in the first pass: skipping the migrating-gate entirely
      passed nothing new (a whole test failed, good), but the dedup merge's
      `if seq > existing` collapsing to a plain overwrite passed EVERY
      existing test, because every one of them only ever migrated a
      ClientID this recipient had never independently seen — a dedicated
      test now pre-seeds a higher SeqNum on the recipient before migrating
      in a stale, lower one for the same client, and checks it doesn't
      regress. No garbage collection yet (Day 6's explicit "challenge"
      scope) — the old owner keeps a moved shard's data forever, unused.
- [x] **Day 6 — Concurrent clients through reconfiguration.** Many clients
      hammering keys while groups join and leave. The invariant is Stage 2's,
      now with data moving underneath: every acknowledged write is visible,
      none lost, none applied twice. Includes garbage-collecting a shard from
      its old owner once the new owner has it (the "challenge" exercise —
      without it, every migration leaks the shard forever).
      GC: a Config that COSTS a group a shard marks it `leaving` (mirroring
      `migrating` on the gaining side); `gcLoop` confirms the new owner is
      actually ready (`HasShard`, itself gated identically to `ownsLocked`)
      before Proposing a `GC` entry that drops the shard's keys — through
      the log, so every replica agrees on exactly when. Deliberately
      conservative: a shard reassigned again before the FIRST recipient ever
      confirms is a known, accepted leak (safe, just not maximally live —
      this day closes the common case, not arbitrary reassignment chains).
      Stress test: 5 private-key clients Appending concurrently while the
      cluster grows 1 group -> 3, two shards Move explicitly, and the
      original group Leaves entirely, all mid-flight; every client's final
      value is checked against the exact concatenation of its own
      acknowledged writes. 15/15 clean, ~150ms each. A mutation
      (`recipientReady` always "true") was only flaky-caught by the
      integration test (some runs passed) — added a deterministic unit test
      of the gate itself, the same "test the authority, not the common
      path" lesson from Day 4, now generalized to "a real bug can be timing
      -dependent to observe; test the gate directly when it can be."
- [ ] **Day 7 — A history checker.** The private-key-per-client trick every
      test so far uses cannot detect cross-client anomalies, and sharding
      creates new ones. Record every operation's `(invoke time, return time,
      argument, result)` and check per-key linearizability of the whole run
      (a Wing-Gong-style search). Validate the checker itself first — it must
      reject hand-built bad histories (stale read, lost write, duplicate
      apply), since a checker that accepts everything is worse than none.
- [ ] **Day 8 — Full integration.** Reconfiguration + concurrent clients + fault
      injection (group crashes, partitions, restarts) + snapshotting, all at
      once, judged by Day 7's checker. Whatever the earlier stages' known
      gaps are (Stage 2's stale-partitioned-leader `Get`) get their honest
      test here.

## Done means

- `go test ./05-sharded-kv/... -race` passes, including the Day 8 integration suite.
- Stage 5 README's concept notes are filled in for every day.
- `PROGRESS.md` updated, Stage 6 scoped next.
