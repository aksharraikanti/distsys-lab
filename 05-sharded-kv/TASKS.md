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

- [ ] **Day 1 — Static sharding.** A fixed shard count (`NShards`, e.g. 10), a
      `key2shard` function, several independent Stage 2 clusters as groups,
      and a `ShardedClient` that routes each key to the group owning its
      shard under a config supplied at construction — no controller, no
      migration yet. Prove keys spread across groups, that the same key
      always routes to the same group, and that a single-group failure only
      affects the shards that group owns. Benchmark write throughput with 1
      vs 3 groups: sharding's whole promise is that writes scale, and it's
      worth measuring whether they do (Raft replication latency per group is
      the floor — throughput should scale, latency shouldn't drop).
- [ ] **Day 2 — Configurations and rebalancing (pure logic).** The `Config`
      type (`Num`, `Shards [NShards]int` group ids, `Groups map[int][]string`)
      and `Join`/`Leave`/`Move` as PURE functions from config to config. The
      rebalance must be deterministic (every replica of the controller will run
      it and must get the identical answer — Go map iteration order is random,
      so this needs care) and must move the MINIMUM number of shards. Property
      tests over random join/leave sequences: every shard always assigned to a
      live group, load balanced to within 1 shard, minimal movement.
- [ ] **Day 3 — The shard controller.** Make the config history a replicated
      service: a Raft-backed state machine (reusing Stage 2's apply-loop,
      dedup, and leader-change machinery) with `Join`/`Leave`/`Move`/`Query`.
      `Query(num)` returns an old config, `-1` the latest. Clerk-style client
      with retries. This is Stage 2 again in a new costume, and the test is
      whether the machinery really was reusable or whether Stage 2 was
      quietly specific to a string map.
- [ ] **Day 4 — Groups serve only their shards.** Each group polls the
      controller for the current config and serves a key only if its shard is
      assigned to it, otherwise `ErrWrongGroup` (the client refetches the
      config and retries). Ownership must be checked through the group's Raft
      log, not a local variable: a leader can lose a shard while a stale
      belief lingers — the same hazard as Stage 2's stale-leader `Get` gap,
      except here it can hand out a shard's data after the shard has moved.
      Decide how reads handle it (read through the log, or a lease).
- [ ] **Day 5 — Shard migration.** When a group's config changes, the new owner
      PULLS the shards it gained from their previous owner, and the dedup
      table moves WITH the shard (otherwise a client retry that spans a
      migration double-applies — Stage 2 Day 3's bug, reintroduced). Config
      changes are themselves log entries, so every replica in a group agrees
      on exactly when a shard changed hands. Operations on a shard mid-move
      must wait or fail with `ErrWrongGroup`, never be lost or served stale.
- [ ] **Day 6 — Concurrent clients through reconfiguration.** Many clients
      hammering keys while groups join and leave. The invariant is Stage 2's,
      now with data moving underneath: every acknowledged write is visible,
      none lost, none applied twice. Includes garbage-collecting a shard from
      its old owner once the new owner has it (the "challenge" exercise —
      without it, every migration leaks the shard forever).
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
