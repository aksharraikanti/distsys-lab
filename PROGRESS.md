# Progress

Current stage: **04-caching**
Current day: **Day 5 — Invalidation races and stampedes** (next up)
Status: Stage 1 (Raft consensus from scratch) complete, all 12 days.
Stage 2 (Fault-tolerant KV store on Raft) complete, all 8 days.
Stage 3 (Connection pooling layer) complete, all 6 days.
Stage 4 (Caching layer) scoped into 6 days, Days 1-4 complete.

## Log

- 2026-09-07 — Repo scaffolded via `/office-hours`. Stage 1 broken into 12 days. Not yet started.
- 2026-09-07 — Day 1 (RPC scaffolding) complete: RPC types, `Transport` interface,
  `FakeTransport` (in-process) and `NetTransport` (real net/rpc over TCP) both
  implemented, 3-node round-trip tests passing under `-race`.
- 2026-09-08 — Day 2 (Server states) complete: Follower/Candidate/Leader state
  machine with enforced transition rules, mutex-protected shared state, and a
  concurrent race test (RPC handlers + simulated election-timer firing at the
  same instance) clean under `go test -race`.
- 2026-09-08 — Day 3 (Election timeouts) complete: randomized per-node election
  timeout, reset/stop signaling, Follower -> Candidate on timeout. Timing tests
  use condition polling (`waitFor`) rather than fixed sleeps.
- 2026-09-08 — Day 4 (Leader election) complete: real RequestVote logic (term
  comparison, one-vote-per-term, log up-to-date check), concurrent vote-counting
  in `startElection`, and an end-to-end test proving 3 nodes converge on exactly
  one leader via their real election timers. Ran long as predicted — the
  concurrency of the election, not the RPC rules themselves, was the hard part.
- 2026-09-09 — Day 5 (Heartbeats) complete: real AppendEntries logic (mirrors
  Day 4's RequestVote pattern), leader-side periodic heartbeat loop
  (`RunHeartbeats`), and a caught bug — `HeartbeatInterval` had equaled
  `ElectionTimeoutMin` since Day 1, dropped to 2ms so heartbeats reliably beat
  the election-timeout floor. `TestHeartbeatsKeepLeaderStable` proves leadership
  now survives many election-timeout cycles once heartbeats are wired up.
- 2026-09-09 — Day 6 (Election edge cases) complete: no new production code —
  every edge case (split vote, stale-leader rejection, term monotonicity, node
  restart) was already correctly handled by Days 2-5's primitives. Added tests
  proving each one, including a genuinely new coverage gap found along the way
  (the leader-side step-down branch in `sendHeartbeats` had never been
  exercised). Node restart is explicitly scoped to liveness only — the real
  safety gap (a restarted node has no memory of its prior vote) is Day 11's job.
- 2026-09-10 — Day 7 (Log entries) complete: `Propose` (leader-side, appends a
  client command to the leader's own log only) and real `AppendEntries` append
  logic (follower-side, trusts PrevLogIndex without verifying it — that
  verification is explicitly Day 10's job). Nothing calls AppendEntries with
  real entries yet; that wiring is Day 8's.
- 2026-09-10 — Day 8 (Log replication) complete: `nextIndex`/`matchIndex`
  reinitialized on every `becomeLeaderLocked`, `sendHeartbeats` renamed to
  `replicate`/`replicateToPeer` (same call site, now carrying real payload
  instead of always-empty entries), and nextIndex-based incremental sending
  proven via a recording transport (a follower's second round only carries
  what's new, not the whole log again). "Retries on failure" is proven with an
  injected always-rejecting handler, since Day 10's consistency check doesn't
  exist yet to make real rejections reachable.
- 2026-09-10 — Day 9 (Commit rule) complete: `advanceCommitIndexLocked`
  implements the Figure 8 safety fix (never commit an older-term entry
  directly, even with a majority — only a later current-term entry reaching a
  majority makes everything before it safe). Follower-side commitIndex now
  tracks LeaderCommit (capped at its own last log index). `RunApplyLoop` runs
  identically on every node — leader and followers alike — delivering
  committed entries on `ApplyCh` in order. `TestEndToEndProposeCommitApply`
  proves the full pipeline: Propose -> replicate -> majority -> commit ->
  LeaderCommit -> apply, landing the same entry on all 3 nodes.
- 2026-09-13 — Day 10 (Log consistency check) complete: real PrevLogIndex/
  PrevLogTerm verification in AppendEntries, rejecting (rather than blindly
  trusting) when a follower's log doesn't actually agree with the leader at
  that point. This is the day three earlier "not yet, later" notes converged
  on: Day 7's naive trust-the-caller append, Day 8's previously-unreachable
  rejection/backoff path, and the defensive bounds guard from Day 9's apply
  loop. `TestReplicateConvergesADivergedFollower` proves a genuinely diverged
  follower (not just behind, but holding a conflicting entry) gets corrected
  via real rejection-and-backoff, not a lucky one-round overwrite.
- 2026-09-13 — Day 11 (Persistence) complete: `Persister` interface with
  `MemoryPersister` (tests) and `FilePersister` (real disk, temp-file +
  fsync + atomic rename + directory fsync — three separate crash-consistency
  windows, all closed). `currentTerm`/`votedFor`/`log` persist at 5 mutation
  sites, unconditionally, before each mutation becomes externally visible.
  `NewRaftWithPersister` is a new opt-in constructor — `NewRaft`'s signature
  is untouched, so the full existing suite (Days 1-10) passed unchanged.
  `TestGrantedVoteSurvivesRestart` closes the exact safety gap Day 6's
  `TestNodeRestartRejoinsCluster` explicitly flagged as open.
- 2026-09-14 — Day 12(-13) (Fault injection tests) complete, and with it,
  **Stage 1 is done**. Ran in one day, not two, because Days 2-11 had already
  built every mechanism this day exercises in combination — the only new
  code was `FakeTransport.Partition`/`Unregister`, the fault-injection
  capability itself, inferring "who's calling" from RPC fields (CandidateID/
  LeaderID) that already existed since Day 1 for unrelated reasons. Three
  integration tests cover every fault TASKS.md named: a crashed leader (new
  leader elected, cluster keeps committing), a network partition (5-node
  cluster splits 3-2, majority elects its own leader and commits, the
  isolated old leader's `Propose` calls succeed locally but never commit,
  healing converges everyone), and a follower restart mid-operation (with
  real persistence — replication catches it up on what happened while it was
  down, since persistence alone only recovers pre-crash state).
  `assertLogsConsistent` checks the Log Matching Property directly: every
  node's log must agree through the lowest commitIndex anyone has reached.
- 2026-09-14 — Stage 2 scoped into 8 days (`02-kv-store/TASKS.md`), following
  MIT 6.5840 Lab 3's structure the same way Stage 1 followed Lab 2. Day 1
  (Apply-loop scaffolding) complete: `Op` (Put/Append, gob-registered in this
  package's own `init()` — the exact forward-pointer Stage 1's Day 11 left),
  `KVServer` wrapping a `*raft.Raft` with an independent apply loop per node.
  `TestKVStoreConvergesAcrossCluster` proves 3 nodes' independently-running
  apply loops converge to the identical map with zero coordination between
  them — Raft's commit-order guarantee is the only thing making that true.
  `Get` is explicitly NOT linearizable yet (a direct local read, no Raft
  routing, no leader check) — that's flagged in its own doc comment for a
  later day to address.
- 2026-09-14 — Day 2 (Client-facing RPC handlers) complete: real `Get`/
  `PutAppend` RPC handlers, and a per-index notify-channel mechanism
  (`notifyChans map[int]chan Op`) that lets `PutAppend` learn exactly which
  command landed at the log index it proposed — not just that commitIndex
  advanced, since a later leader's entry can legitimately supersede this
  node's still-uncommitted one at the same index. `commitTimeout` bounds how
  long a handler waits before giving up.
  `TestPutAppendDetectsSupersededProposal` proves the supersession case
  end-to-end with real election/partition machinery; writing it surfaced a
  genuine test bug (both nodes starting at term 0 meant a single
  `BecomeCandidate()` call only tied terms instead of exceeding them — fixed
  with two calls on the challenger). Stress-testing also surfaced a
  ~13%-flaky pre-existing Stage 1 test (`TestHeartbeatsKeepLeaderStable`)
  under real CPU contention from both packages' test suites running
  together — flagged as a separate follow-up task, not folded into this PR.
- 2026-09-15 — Fixed the flagged Stage 1 flakiness directly (not part of
  either stage's TASKS.md — a standalone reliability fix): widened
  `ElectionTimeoutMin`/`Max` from 10x/50x `HeartbeatInterval` to 20x/100x, and
  tightened `TestResetElectionTimerPreventsTimeout`'s own reset-ticker margin.
  Reduced local dual-package-contention flakiness from ~13% to ~4% across
  repeated stress runs; not fully eliminated (true elimination would need a
  fake-clock rewrite disproportionate to a reliability fix), and CI itself has
  never shown this across 15+ real PR runs.
- 2026-09-15 — Day 3 (Duplicate request detection) complete: `Op` gained
  `ClientID`/`SeqNum`, and `applyLoop`'s dedup check (`op.SeqNum >
  duplicateTable[op.ClientID]`) lives in the state machine itself, not the RPC
  layer, so every replica computes the identical dedup decision from the
  identical committed log. Adding the fields broke ~12 existing test call
  sites whose unset `SeqNum` defaulted to Go's zero value (0), which the new
  check would treat as an already-seen duplicate — fixed by giving every call
  site a real `SeqNum` starting at 1, not a `SeqNum == 0` bypass (a real
  dedup-evading loophole). `TestStaleRetryAfterNewerRequestSuppressed` proves
  the comparison must be strict `>`, not `!=`/`==` — a subtly wrong version
  would pass every other test in the file.
- 2026-09-15 — Day 4 (Leader-change correctness) complete: `PutAppend` now
  polls (`leaderCheckInterval`) whether it's still leader of the term it
  Proposed in, while waiting for its entry to commit — a fast bailout for the
  case Day 2's supersession check couldn't catch: this node loses leadership
  and NOTHING ever lands at that log index again, so the notify channel never
  fires and the only fallback was the full `commitTimeout`. Not a new
  correctness guarantee (Day 2 already got there eventually via timeout) — a
  much faster path to the same conclusion.
  `TestPutAppendBailsOutQuicklyWhenLeadershipLost` proves the fast path;
  `TestPutAppendSucceedsWhenLeadershipNeverLost` guards against the new
  polling loop misfiring into a false failure on an ordinary successful
  commit.
- 2026-09-15 — Day 5 (Concurrent client stress test) complete: `Clerk` (retry
  client, ClientID+SeqNum per Day 3's contract) and two stress tests —
  many-clients-no-faults, and many-clients-through-real-fault-injection reused
  from Stage 1 Day 12's `FakeTransport`. Found and fixed three real bugs along
  the way, none of which any earlier single-threaded test could have caught:
  (1) a freshly-elected leader serving stale/missing reads until something in
  its own term commits, per Day 9's Figure 8 rule — fixed with the Raft
  paper's own §8 technique, a no-op entry proposed once per newly-observed
  leadership term (`noopLoop`); (2) `applyLoop`/`noopLoop` goroutines with no
  shutdown path, leaking across the whole test binary and contending for
  scheduler time once enough orphaned `KVServer`s piled up — fixed with a
  `stopCh`/`stopOnce`/`Stop()` shutdown, same shape as `raft.Raft`'s existing
  `StopElectionTimer`; (3) `Clerk.NewClerk` seeding `math/rand` from
  `time.Now().UnixNano()`, which let two `Clerk`s constructed in the same
  wall-clock nanosecond draw the identical ClientID — corrupting
  `duplicateTable`'s dedup tracking so one client's write silently no-opped
  while still reporting `OK`, since the notify channel fires on the proposed
  op matching regardless of whether the dedup check let the mutation through.
  Root-caused by tracing one ClientID through debug output and finding it
  shared across two different clients' Propose calls; fixed by drawing
  ClientID from `crypto/rand` instead. 25/25 clean stress runs and 10/10 clean
  full-suite runs under `-race` after the fix.
- 2026-09-17 — Day 6 (Snapshotting) complete: `KVServer` now serializes
  `store`+`duplicateTable` together (`kvSnapshot`) once `rf.RaftStateSize()`
  crosses `maxRaftState`, handing the bytes to a new `raft.Raft.Snapshot`.
  Most of the actual work landed in `01-raft`, not `02-kv-store`: every
  place that indexed the log directly (`AppendEntries`,
  `advanceCommitIndexLocked`, `applyPending`, `replicateToPeer`,
  `lastLogInfoLocked`, `Propose`) assumed a log's physical position and its
  paper-style absolute index were the same number — an assumption
  compaction breaks outright once `r.log` only holds entries after
  `lastIncludedIndex`. Added `physicalIndexLocked`/`termAtLocked` and
  threaded them through every one of those sites. `Persister` gained
  `SaveStateAndSnapshot`/`ReadSnapshot` — snapshot written BEFORE state,
  deliberately, since state is what "commits" the compaction
  (`LastIncludedIndex/Term`), and the reverse order risks a claim on disk
  with nothing backing it up if a crash lands in between. A follower whose
  `nextIndex` falls at or below `lastIncludedIndex` is a known, documented
  gap left for Day 7's `InstallSnapshot` RPC — `replicateToPeer` just skips
  that peer for the round rather than send a doomed `AppendEntries`. 15/15
  clean full-suite runs under `-race` after landing.
- 2026-09-17 — Day 7 (InstallSnapshot RPC) complete: a new Raft RPC that
  catches up a follower whose `nextIndex` has fallen at or below the
  leader's `lastIncludedIndex` — `replicateToPeer` now calls
  `sendInstallSnapshot` for that peer instead of skipping it. Two real bugs
  found and fixed along the way, both via a full 3-node fault-injection test
  rather than the unit tests alone: (1) `InstallSnapshot` sent directly on
  `ApplyCh` from the RPC-handler goroutine, a second sender racing
  `applyPending`'s own goroutine on the same channel — could deliver a
  regular entry after a newer snapshot already superseded it. Fixed by
  queuing the snapshot (`pendingSnapshot`) and having `RunApplyLoop`'s own
  goroutine deliver it, restoring "exactly one sender." (2) `Snapshot`'s
  documented precondition ("already applied through index") was never
  actually checked — a caller snapshotting a moment before `applyPending`
  caught up could silently corrupt the log; added the missing
  `index > lastApplied` check. Also gave Raft its own in-memory
  `snapshotData` field (independent of whether a persister is attached) —
  the leader-serving-a-peer path has nothing to do with a persister, and the
  common persister-less case would otherwise send an empty snapshot despite
  having genuinely compacted its log. `KVServer.applyLoop` now handles
  `msg.SnapshotValid` by restoring `store`/`duplicateTable` wholesale.
  20/20 clean full-suite runs under `-race` after landing.
- 2026-09-19 — Day 8 (Full integration) complete, and with it, **Stage 2 is
  done**. Two tests: a 5-node cluster running concurrent clients through
  crashes AND partitions with a low `maxRaftState` forcing real snapshotting
  (the combination that finally exercises `InstallSnapshot` under real
  concurrent load), and a separate whole-cluster restart test (every node's
  Raft+KVServer discarded and rebuilt from persisted state, snapshot
  included — split into its own test since `Clerk` calls `KVServer` directly
  with no RPC boundary to survive a mid-test object swap through). The
  restart test's first run failed on the very last fragment of the very
  last key — not data loss, but a freshly re-elected leader's volatile
  `commitIndex` not yet having caught back up via `noopLoop`'s
  re-confirmation (Day 5's own known characteristic, seen for the first
  time in a whole-cluster-restart context); fixed by waiting for
  `CommitIndex` to catch up before trusting a `Get`, not by changing
  production code. 30/30 clean isolated runs and 20/20 clean full-suite runs
  under `-race`.
- 2026-09-19 — Stage 3 (Connection pooling layer) scoped into 6 days
  (`03-connection-pooling/TASKS.md`): every earlier stage's `Clerk` has
  called `KVServer` directly, in-process, so this stage starts by putting a
  real `net/rpc`-over-TCP boundary back in (mirroring 01-raft's own
  `NetTransport`), then builds a bounded connection pool on top of it —
  backpressure under exhaustion, health-checked eviction of connections to a
  crashed node, idle eviction and pool sizing, and a final load test
  combining Stage 2's own concurrent-client and fault-injection patterns
  with the pool sitting in between.
- 2026-09-19 — Day 1 (Real network boundary) complete: `ServeKVServer`
  exposes a `KVServer` over real TCP via `net/rpc` (no wrapper type needed —
  `Get`/`PutAppend` already had the exact shape `net/rpc` requires, unlike
  01-raft's `raftRPCService`, since `KVServer` never sat behind an
  interface). `NaiveClient` dials a fresh connection per call — no pooling —
  and turned out nearly line-for-line identical to `Clerk`'s own retry
  logic; the only real difference is that a dial/call can now fail outright
  (a real socket, not an in-process call), treated the same way `Clerk`
  already treats an unreachable peer. `BenchmarkNaiveClientPutAppend`
  measures the cold-start baseline every later day's pooling has to beat:
  ~3.1ms/op on a 3-node loopback cluster.
- 2026-09-21 — Day 2 (Naive fixed-size pool) complete: `Pool` dials `size`
  connections to one address up front and hands them out via a
  channel-backed free list; `PooledClient` is the same retry/dedup logic
  `NaiveClient` uses, now factored into a shared `client` type plus a
  one-method `caller` interface (`dialPerCallCaller` vs `pooledCaller`) so
  it isn't duplicated a second time. The benchmark comparison split cleanly
  by operation: `PutAppend` barely improved (~3.1ms either way — a write's
  latency floor is Raft's own replication round trip, not connection
  setup), `Get` improved ~2-3x (~150-220μs down to ~50-80μs — no Raft round
  trip underneath a read, so the client's own connection cost is nearly the
  whole story). Also found (via -race, sharing one `PooledClient` across
  goroutines) and documented that `PooledClient` carries the exact same
  "not safe for concurrent use" contract `Clerk` already does. Along the
  way, stress-running the suite surfaced and fixed a real gap in Day 8's
  `TestFullIntegrationSurvivesWholeClusterRestart` (02-kv-store): it waited
  for `CommitIndex` to catch up post-restart but not the further lag
  through `applyPending`/`KVServer.applyLoop` before `Get` actually
  reflects it — fixed by polling the real observable outcome instead.
- 2026-09-21 — Day 3 (Backpressure under exhaustion) complete: `Pool.Call`
  now blocks up to a `checkoutTimeout` before returning `ErrPoolExhausted`,
  replacing Day 2's implicit unbounded block with a deliberate, tested
  policy — chosen over an overflow queue (just relocates unbounded growth
  rather than bounding it) or immediate rejection (treats "busy" the same
  as "broken," failing a burst it could have absorbed).
  `defaultCheckoutTimeout` is derived from `raft.HeartbeatInterval` (10x
  it), matching `client.go`'s own retry-sleep constant, so the pool's wait
  and the client's own retry cycle aren't picked independently of each
  other.
- 2026-09-23 — Day 4 (Health checking and eviction) complete: `Pool.Call`
  now evicts and replaces a connection that failed at the transport level,
  instead of always returning it to the free list. Detection needed no
  special-casing — `KVServer.Get`/`PutAppend` never return a non-nil Go
  error (outcomes travel through `reply.Err`), so any error `Call` itself
  returns is necessarily transport-level. A redial that fails immediately
  (the node genuinely still down, not just a stale connection) hands off to
  a background `redialUntilSuccess` goroutine, paced by
  `defaultRedialInterval` (`raft.ElectionTimeoutMin`) — without it, a node
  held down for real time (Stage 2's own fault injection) could leave the
  pool permanently short a connection even after the node recovered.
  `Close` gained its own `stopCh`/`sync.WaitGroup` to shut that goroutine
  down cleanly before draining the free-list channel. Also found, while
  writing the recovery test, that closing a `net.Listener` does NOT affect
  already-accepted `net/rpc` connections — simulating a broken connection
  has to happen client-side to be realistic at all. 25/25 clean isolated
  pool-test runs and 20/20 clean full-suite runs under `-race`.
- 2026-09-21 — Day 5 (Idle eviction and pool sizing) complete: `Pool` is
  now elastic between `MinSize`/`MaxSize` instead of a single fixed count —
  `checkout` dials a fresh connection on demand when nothing's idle and
  there's still room under `MaxSize`, and a background evictor closes
  connections above `MinSize` that have sat idle past `IdleTimeout`.
  `NewPool` grew enough same-typed `time.Duration` parameters across Days
  3-5 to become a real (not hypothetical) footgun — replaced with a
  `PoolOptions` struct. Also revised Day 4's "always replace a broken
  connection" guarantee: above `MinSize`, a broken connection now just
  shrinks the pool by one instead of always triggering a redial —
  elasticity makes "always replace" the wrong default once the pool can
  legitimately be larger than it strictly needs. Proved growth, shrinkage,
  and regrowth both in isolation and through a real load/idle/load cycle
  with actual concurrent `Call` traffic. 30/30 clean isolated pool-test
  runs and 20/20 clean full-suite runs under `-race` (one pre-existing,
  already-documented Stage 1 flake — `TestHeartbeatsKeepLeaderStable` —
  unrelated to this day, seen once).
- 2026-09-23 — Day 6 (Load test through real faults) complete, and with it,
  **Stage 3 is done**. `TestPoolLoadThroughRealFaults`: six `PooledClient`s
  (pools of 1-2, fewer than callers, so backpressure is real) hammering a
  real-TCP 3-node cluster while a node's TCP endpoint crash/restart (every
  connection severed), a leader cut-off, and a partition fire in rotation;
  asserts at least three fault rounds actually ran, no connection leaked,
  every pool recovered, and a fresh client reads everything back. Its first
  version passed in 0.12s because the workload outran the first fault — fixed
  and verified the test can fail (skipping eviction deadlocks it).
  **Found and fixed a real Stage 2 bug:** ~1 run in 8, clients read a value
  missing their last acknowledged Append (always converged later — a stale
  read, not a lost write). Cause: a just-elected leader served reads before
  applying a no-op from its own term; Day 5's `noopLoop` proposed the no-op
  but `Get` never waited for it. `Get` now gates on `noopAppliedTerm` (Raft
  §8); 1/8 -> 0/50 failures. Also fixed a Day 5 test that read the pool size
  after the load instead of its peak during it. Stage 4 (caching) scoped
  into 6 days (`04-caching/TASKS.md`).
- 2026-09-23 — Stage 4 Day 1 (Cache-aside read path) complete: `Cache` over a
  `Store` interface that `PooledClient` satisfies as-is; unbounded, no expiry,
  writes pass through (deliberately — `TestWriteLeavesCachedValueStale` pins
  the resulting staleness gap until Day 4). Store is called outside the cache
  lock so one slow miss can't block hits. Measured against a real pooled TCP
  cluster: uncached `Get` ~36-43µs, miss ~36µs (no overhead), hit ~16-22ns
  (~2000x). Sharing one client across a cache's callers forced a Stage 3
  change: `client` is now safe for concurrent use — reads concurrent, writes
  serialized, because dedup keeps only the highest SeqNum per ClientID and
  concurrent writes could silently lose the lower one. 20/20 clean
  full-suite runs under `-race`.
- 2026-09-24 — Stage 4 Day 2 (Bounded capacity and LRU eviction) complete:
  `New(store, capacity)` keeps at most `capacity` entries in a map + doubly
  linked recency list (O(1) lookup and O(1) "mark used"); a hit counts as a
  use, the back of the list is evicted on overflow, and `Evictions` is
  counted. Tests check eviction order step by step and the map/list
  invariant directly; the duplicate-node test was verified by mutation
  (removing `insertLocked`'s existing-key branch fails two tests). A hit
  still costs ~17-22ns. Also fixed Stage 2's `TestPutAppendAndGetRoundTrip`,
  a leftover from Stage 3 Day 6's `Get` gate that flaked once in 20 runs
  (now 300/300).
- 2026-09-25 — Stage 4 Day 3 (TTL expiry) complete: entries carry a deadline
  and an expired entry is a miss (counted as `Expirations` and `Misses`, never
  a hit). Deadline = fetch start + TTL, hits don't extend it, and the deadline
  itself is already expired; each choice has a test, verified by mutation.
  Expiry is lazy (no sweeper goroutine). Time comes from an injected `Clock`
  so tests advance a fake clock instead of sleeping. `New` became
  `New(store, Options{Capacity, TTL, Clock})` before a third positional
  argument could become Stage 3's `NewPool` footgun. TTL costs ~32ns per hit
  (~16ns -> ~48ns); TTL 0 never reads the clock. One mutation check was
  invalid (it didn't compile, so nothing ran) and was redone. 20/20 clean
  full-suite runs under `-race`.
- 2026-09-26 — Stage 4 Day 4 (Write policies) complete: `Options.WritePolicy`
  with `WriteInvalidate` (zero value; drop the entry) and `WriteThrough`
  (replace it). `Append` invalidates under both — the cache can't safely
  compute old+value from a possibly-stale copy, and reading it back costs a
  round trip per Append. Writes go to the store first, then the cache; a test
  freezing a `Put` mid-write proves it and the reordering mutation fails it.
  Compared by counting store reads (a real write's ~3ms of Raft would drown
  timing): write-then-read pairs cost invalidate 200 vs through 0, while
  write-only keys plus a hot read set cost invalidate 0 vs through 287 —
  a real tradeoff. `TestStaleFillRaceIsAKnownGap` pins the reader-fetches-old
  -value race that store-first ordering can't close (Day 5). Read-your-writes
  verified through the real pooled Raft stack. 20/20 clean full-suite runs
  under `-race`. (Docs landed in a follow-up PR after a scripting slip; the
  code shipped in #31.)
