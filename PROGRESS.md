# Progress

Current stage: **06-hot-shard-splitting**
Current day: **Day 5 — Automatic splitting end-to-end** (next up)
Status: Stage 1 (Raft consensus from scratch) complete, all 12 days.
Stage 2 (Fault-tolerant KV store on Raft) complete, all 8 days.
Stage 3 (Connection pooling layer) complete, all 6 days.
Stage 4 (Caching layer) complete, all 6 days.
Stage 5 (Sharded KV store) complete, all 8 days.
Stage 6 (Hot shard detection and splitting) scoped into 6 days, Days 1-4 complete.

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
- 2026-09-27 — Stage 4 Day 5 (Invalidation races and stampedes) complete:
  concurrent misses on a key coalesce into one Store fetch (`flight` registry;
  followers wait, and retry if the leader panics); a write marks the key's
  in-flight fetch stale and DETACHES it, so a racing reader's old value is
  never cached and the writer's own next `Get` can't join it — no per-key
  version map, no unbounded state. Found a third race while designing this:
  concurrent `WriteThrough` writers could update the cache in the opposite
  order from the store, so writes are now serialized (free through the KV
  client, which already serializes per ClientID). Day 4's known-gap test is
  flipped to `TestStaleFillIsDiscarded`. Over the real pooled Raft stack 50
  concurrent misses on one key -> 1 cluster read. A whole-system property test
  checks the cache never disagrees with the store after concurrent traffic.
  Each of the five guards is verified by mutation (two invalid attempts,
  a non-compiling and a hanging one, were caught and redone; the detach test
  was rewritten to fail fast instead of hang). 20/20 clean full-suite runs
  and 100x on the race tests under `-race`.
- 2026-09-28 — Stage 4 Day 6 (Load test, hit rate, and faults) complete, and
  with it, **Stage 4 is done** — the track's first demoable milestone: a
  connection-pooled, cached, Raft-backed KV store. Zipf reads over the real
  stack: a cache holding 25% of the keys hits 79.5%; median read 155µs ->
  1.3µs (under `-race`); hit rate provably monotonic in capacity (LRU is a
  stack algorithm). `TestCacheUpholdsItsInvariantsThroughFaults` runs writers
  (read-your-writes) and concurrent readers (constant shared keys, well-formed
  writer keys) through endpoint crashes, leader cut-off and partitions under
  both write policies; reintroducing the stale-fill or no-detach bugs makes it
  fail 3/3. Its first version passed with `coalesced=0 staleDiscarded=0` —
  writes serialized and nothing raced — and was rebuilt around dedicated
  readers. `TestCacheServesHitsDuringTotalStoreOutage`: with every endpoint
  down and a fetch and a write blocked, hits are still answered (no lock is
  held across a store call). Honest limit: `Store` has no error return, so
  "fail fast" would need one. My unthrottled readers pinned cores and starved
  Stage 1's timing tests; throttled. Stage 1 timing tests
  (`TestHeartbeatsKeepLeaderStable`, `TestAppendEntriesResetsElectionTimer`)
  still fail ~1-4% of full-suite runs — flagged for a dedicated fix. Stage 5
  (sharded KV store) scoped into 8 days (`05-sharded-kv/TASKS.md`).
- 2026-09-28 — Fix (Stage 3): pool could exceed MaxSize and hang `Close()`.
  Found because Stage 4 Day 6's fault test hung for 5 minutes once in ~15
  runs on a `GOMAXPROCS=2` CI emulation; the goroutine dump showed the idle
  evictor blocked sending into a full free list. Cause: the redial paths
  checked `count < MinSize`, dialed unlocked, then incremented — so concurrent
  redialers plus growth could push count to MaxSize+1. Fixed by reserving the
  slot before dialing (`tryReserveRestore`). Two stress tests failed to
  reproduce it; a deterministic test using an injectable `dial` seam did
  (`count = 3 > MaxSize 2` on the old logic). CI-like full suite: 1/15 before,
  0/25 after; the cache fault test alone 0/40. Also corrects my Stage 4 Day 6
  merge: a CI check FAILED (Stage 2's `TestKVStoreConvergesAcrossCluster`, a
  1-second timing window) and I merged anyway because my shell chain piped
  `gh pr checks` through `tail`, discarding its exit code. Merges are now gated
  on the check result.
- 2026-09-29 — Stage 5 Day 1 (Static sharding) complete: `NShards = 10`,
  `Key2Shard` (FNV-1a; 10,000 prefixed keys within ±2% of the mean per shard),
  `Config{Num, Shards, Groups}` with `NewStaticConfig` (deterministic,
  round-robin, sorted by group id) and `Validate`, and `ShardedClient` — one
  pooled client per group, safe for concurrent use. Real-cluster tests: keys
  live only in their owning group (checked by asking each group directly);
  with one group down, the other groups serve reads and writes normally while
  the dead group's key blocks, then completes with pre-outage data on
  recovery. Write throughput 270/s (1 group) -> 804/s (3 groups), 2.97x.
  Mutation-checked (wrong shard function, first-byte hash, global lock on
  reads, global lock on writes); the checks exposed an unbounded search in my
  isolation test that turned a routing bug into a 2-minute hang, now bounded.
  Extracted the crashable-endpoint cluster harness into `internal/kvtest` (its
  third copy); the Stage 3/4 copies are not yet migrated. 30/30 clean stage
  runs and 20/20 clean full-suite runs under `GOMAXPROCS=2 -race`.
- 2026-09-28 — Stage 5 Day 2 (Configurations and rebalancing) complete:
  `Join`/`Leave`/`Move` as pure `Config -> Config` functions, and `rebalance`
  (sorted group ids for a deterministic remainder split, dead/over-target
  shards become orphans, orphans handed out in ascending shard order). Group
  id 0 reserved as "unassigned" so `Leave`-ing every group and bootstrapping
  `Join` from `Config{}` both just fall out of `Validate`'s existing checks.
  Pinned an exact case: 3 balanced groups (4/3/3) + a 4th rebalances to
  3/3/2/2 moving exactly 2 shards. 200 random Join/Leave sequences checked
  for full routability and within-1-shard balance after every step, not just
  the end. Mutation-checked: assigning the remainder to the last groups
  instead of the first still balances but moves an extra shard (caught by the
  exact-2 assertion, not the balance check); treating a dead group's shards
  as still-owned panics immediately on the first Join from empty (a loud
  failure, not a silent one). 20/20 clean full-suite runs under `-race`.
- 2026-09-28 — Stage 5 Day 3 (The shard controller) complete: `Ctrler`, a
  Raft-replicated log of `Config` versions — 02-kv-store's apply-loop, dedup
  table, notify channels, and no-op-on-election machinery, reused unchanged,
  driving `configs = append(configs, Join/Leave/Move(latest, ...))` instead
  of a map write. `Query` reuses `Get`'s exact Raft §8 gate. `CtrlerClerk` is
  an in-process retry client (real RPC serving is Day 4's job, once groups
  actually need to poll the controller). One real gap found by mutation
  testing: removing `Query`'s §8 gate broke nothing in the initial suite —
  no existing test forced the window between "leader" and "own no-op
  applied" open long enough to observe it — so a whitebox test was added
  that builds a `Ctrler` by hand around a node forced straight to Leader,
  with `noopLoop` never started, to freeze that window on purpose. Dedup and
  leader-cutoff mid-Join are also mutation-verified. 15/15 clean stage runs
  and 3/3 clean full-suite runs under `GOMAXPROCS=2 -race`.
- 2026-09-28 — Stage 5 Day 4 (Groups serve only their shards) complete:
  `GroupServer`, a new type polling the controller in-process (a real net/
  rpc face turned out not to be needed yet either — revised from Day 3's own
  prediction). A leader Proposes exactly `cfg.Num+1` as a `Config` log entry
  (never the latest — configs adopt one at a time, for Day 5's sake); every
  Put/Append is checked against `cfg` at the moment it APPLIES, not whenever
  proposed. `Get` reuses `KVServer`/`Ctrler`'s Raft §8 gate — "read through
  the log," TASKS.md's own suggested answer over a lease. Mutation testing
  found two real gaps: `PutAppend`'s pre-check (a fast-path optimization)
  masked the apply-time ownership check in every test that used it, so a
  dedicated test proposes a Config directly ahead of a Put on one leader to
  force the exact race the pre-check can't close; and `Config.Num == cfg.Num
  +1` relaxed to `>` also passed everything, since the poll loop's own
  discipline never generates a version-skipping proposal to expose the
  difference — pinned with a direct skip-a-version test. Both gaps share a
  shape: an optimization or a caller-side invariant can fully hide a missing
  check in the code path meant to be the real authority. Inherits Stage 2's
  known partition gap unchanged (a silently-partitioned leader keeps
  answering from its own stale `cfg`) — flagged, not fixed, same as there.
  20/20 clean stage runs and 3/3 clean full-suite runs under
  `GOMAXPROCS=2 -race` (one pre-existing, already-documented Stage 2 flake —
  `TestKVStoreConvergesAcrossCluster` — unrelated to this day, seen once).
- 2026-09-29 — Stage 5 Day 5 (Shard migration) complete: adopting a Config
  that grants a REAL shard (previous owner not 0, not me) marks it
  `migrating` instead of ready; `migrationLoop` (leader-only) Pulls it and
  Proposes a `Migrate` entry once data arrives, landing through the log so
  every replica gets it identically. `Pull` answers only once the donor's
  own applied config has caught up to the transition (data is provably
  frozen by then) AND the donor isn't itself still migrating that shard in
  — the second gate matters if a shard is reassigned again before its first
  migration lands; without it a donor could hand off incomplete data. Whole
  `duplicateTable` travels with every migration (a client's SeqNum sequence
  isn't shard-scoped), merged by MAX per ClientID. Mutation testing found a
  gap that rhymes with Day 4's: the dedup merge's `if seq > existing`
  collapsed to a plain overwrite passed every existing test, since every
  migration in every test landed a ClientID the recipient had never
  independently seen — a dedicated test now pre-seeds a higher SeqNum on
  the recipient before migrating in a stale, lower one for the same client.
  No garbage collection yet (Day 6's own named "challenge" scope) — old
  owners keep a moved shard's data forever, unused. 15/15 clean stage runs
  and 3/3 clean full-suite runs under `GOMAXPROCS=2 -race`.
- 2026-09-30 — Stage 5 Day 6 (Concurrent clients through reconfiguration)
  complete, and with it the Day 6 "challenge": garbage collection. A Config
  that COSTS a group a shard marks it `leaving` (mirroring Day 5's own
  `migrating` on the gaining side, populated by the same code, one loop now
  checking both directions); `gcLoop` confirms the new owner is genuinely
  ready via a new `HasShard` RPC (itself just `ownsLocked`'s own check,
  exposed) before Proposing a `GC` entry that drops the shard's keys —
  through the log, so every replica agrees on when. Deliberately
  conservative: a shard reassigned again before the FIRST recipient ever
  confirms is a known, accepted leak (safe, not maximally live). Stress
  test: 5 private-key clients Appending concurrently while the cluster
  grows 1 group -> 3, two shards Move explicitly, and the original group
  Leaves entirely, all mid-flight — every client's final value checked
  against the exact concatenation of its own acknowledged writes, 15/15
  clean, ~150ms each. One test-only bug found this way: sharing a single
  `*CtrlerClerk` across concurrent client goroutines raced (it's documented
  as unsafe for concurrent use, same as `ShardClerk`) — each client needed
  its own. Mutation testing found the GC deletion itself caught
  immediately, but mutating the `recipientReady` safety gate to always
  return true was only flaky-caught by the integration test (real
  goroutine-scheduling timing, not every run) — added a deterministic unit
  test of the gate directly, generalizing Day 4's "test the authority, not
  the common path" to: when the authority's failure is itself
  timing-dependent, test the gate in isolation. 15/15 clean stage runs and
  3/3 clean full-suite runs under `GOMAXPROCS=2 -race`.
- 2026-10-01 — Stage 5 Day 7 (A history checker) complete: `History`/
  `HistoryEntry` record real `[Invoke, Return)` intervals; `IsLinearizable`
  splits per key (sound — no multi-key ops before Stage 7) and backtracks
  per key, memoized on (placed-set, value) to stay clear of the naive
  O(n!). Validated against hand-built stale read, lost write, duplicate
  apply, and a genuinely concurrent pair of Appends that must accept
  EITHER order. Mutation-checked the checker itself two ways: the
  Get-result comparison collapsed to always-true (the literal "accepts
  everything" risk) was caught by every "Rejects" test; the real-time
  ordering check collapsed to always-false was caught ONLY by the
  stale-read and lost-write tests specifically, whose argument depends on
  real time leaving no other explanation — confirming each test earns its
  place rather than just agreeing by coincidence. Run for real: 4 clients
  on 2 SHARED keys (not private ones) through a live reconfiguration,
  recorded via a new `RecordingShardClerk`, found linearizable — the actual
  demonstration that Days 1-6's design holds up under the one anomaly
  class the private-key trick was structurally blind to. 15/15 clean stage
  runs and 3/3 clean full-suite runs under `GOMAXPROCS=2 -race`.
- 2026-10-01 — Stage 5 Day 8 (Full integration) complete, and with it,
  **Stage 5 is done**. Added real snapshotting to `GroupServer` (`Cfg`/
  `Migrating`/`Leaving` alongside `KVServer`'s own `Store`/`DuplicateTable`
  pattern) — the one mechanism this stage never needed until a day combined
  long-running traffic with everything else. One combined stress test: 4
  clients on 3 shared keys, live reconfiguration, 6 repeated crash/restart
  rounds across 3 groups, real snapshotting (low `maxRaftState`), judged by
  `IsLinearizable` — passes. A dedicated snapshot test forces a follower to
  catch up via `InstallSnapshot` after missing a REAL reconfiguration
  during its outage, not just writes — "leave cfg/leaving untouched" and
  "correctly restore them" are indistinguishable unless something actually
  changed while disconnected, confirmed by mutation testing a first,
  weaker version of the test that didn't notice `s.cfg = snap.Cfg` deleted
  outright. A dedicated "honest gap" test reproduces Stage 2's long-flagged
  stale-partitioned-leader `Get` directly and proves Day 7's checker flags
  the resulting history as non-linearizable — closing the loop between the
  two tools this stage built. Found, while building that test, that
  `FakeTransport.Unregister` alone does not isolate a LEADER (verified
  empirically): it only blocks incoming calls, not the leader's own
  outgoing replication, so a leader "cut off" this way keeps committing
  normally. `Partition`/`Heal` is the correct primitive — used in the
  honest-gap test (15/15 clean reproductions) — and this likely means
  several EARLIER leader-cutoff tests in this stage (Day 3, Day 4) have
  been passing without genuinely forcing the leadership change they claim;
  flagged as a follow-up task rather than fixed today. 15/15 clean stage
  runs and 3/3 clean full-suite runs under `GOMAXPROCS=2 -race`. Stage 6
  (Hot shard detection and splitting) scoped into 6 days
  (`06-hot-shard-splitting/TASKS.md`): a consistent-hashing ring replaces
  Stage 5's fixed `NShards` buckets so a shard can split into two without
  touching any other key's assignment, load tracking and hot-shard
  detection drive automatic splits, and Stage 5's own migration/GC/checker
  machinery is reused as-is once a split is just another config change.
- 2026-10-01 — Stage 6 Day 1 (Consistent hashing ring) complete: `ShardID`
  (a stable identity surviving a future Move, unlike Stage 5's own
  array-index identity, which can't survive a shard being created) +
  `RingEntry{Start, Shard}` + `Ring{Entries}`, sorted ascending so
  `RingAssign` binary-searches; a hash below every stored Start wraps to
  the HIGHEST-Start entry, the actual "ring" property. `Split` is
  self-validating (no replicated boundary exists yet to do that for it).
  Deliberately kept group ownership OUT of `Ring` — a Split and a Move are
  unrelated concerns, and Day 2's load tracking needs load keyed by
  `ShardID` regardless of which group currently serves it. Property-
  verified: every ring point belongs to exactly one shard (checked against
  exact boundary values, not just hashed keys, since a boundary-only bug
  could hide behind thousands of interior-point checks), and splitting a
  shard never changes any OTHER key's owner. Mutation testing found
  `RingAssign`'s own wrap fallback was unreachable through every test
  using `NewRing` (which always starts its first entry at exactly 0, so no
  hash can ever fall "before" it) — refactored into an internal
  point-based `ringAssignPoint` so a hand-built ring could exercise it
  directly. 20/20 clean stage runs under `-race`.
- 2026-10-01 — Follow-up: closed the `Unregister(leaderID)` gap Stage 5 Day
  8 flagged and deferred. Audited every `transport.Unregister` call in
  Stage 3 and Stage 5 that targets whichever node is CURRENTLY leader
  (not a generic/follower node): Stage 5's `TestCtrlerSurvivesLeaderChange`
  (Day 3) and `TestGroupServerWriteSurvivesLeaderCutoff` (Day 4), plus the
  identical pattern in Stage 3's `TestPoolLoadThroughRealFaults` (Day 6,
  its "the Raft leader is cut off" fault). All three switched from
  `Unregister`/`Register` to `Partition`/`Heal`. Verified the fix had real
  teeth before trusting it: a throwaway diagnostic ran each primitive 15
  times against a 3-node controller cluster and checked whether the
  leader's term or identity ever actually changed — `Unregister`: 0/15;
  `Partition`: 1/15. The low `Partition` rate isn't a bug, it's the race
  both tests document on purpose ("win or lose the race with commit"); the
  load-bearing number is that `Unregister` was provably always zero,
  confirming the retry-to-new-leader path these tests are named for had
  never once actually fired. Reran the two Stage 5 tests 20x and the
  Stage 3 test 5x, all under `-race`: clean. Stage 5's own
  `integration_test.go` has two more `Unregister` calls that were
  deliberately left alone — one targets a named follower on purpose, the
  other round-robins through every replica id "so both leaders and
  followers get crashed," which is the point, not a bug. Stage 2's
  `integration_test.go`/`stress_test.go` have the same leader-targeting
  pattern and were NOT touched — out of scope for this pass, left for a
  later follow-up. Full `go test ./... -race` clean.
- 2026-10-01 — Follow-up: closed the Stage 2 half of the same
  `Unregister(leaderID)` gap, left out of the previous entry on purpose.
  Confirmed both `TestConcurrentClientsWithFaultInjection` (Day 5) and
  `TestFullIntegrationConcurrentClientsFaultsAndSnapshotting`'s "Crash
  fault" branch (Day 8) target whichever node is CURRENTLY leader
  specifically, same shape as the already-fixed Stage 3/5 tests. Switched
  both from `Unregister`/`Register` to `Partition`/`Heal`. Same
  verification approach: a throwaway diagnostic on a 3-node cluster, 15
  runs each, checking whether the leader's term or identity ever actually
  changed — `Unregister`: 0/15; `Partition`: 15/15 (every run forces a new
  leader here, unlike Stage 3/5's 1/15, because a 3-node cluster's cut-off
  leader can never win the commit race the way a larger cluster
  sometimes does — it's isolated from the only other majority-capable
  peers immediately). `install_snapshot_test.go`'s `Unregister` call was
  left alone — it targets a named lagging follower, not the leader.
  Reran both affected tests 10x under `-race`: clean. Full
  `go test ./... -race` across the repo: clean.
- 2026-10-01 — Stage 6 Day 2 (Per-shard load tracking) complete:
  `LoadTracker` — per-`ShardID` counters, `Record`/`Snapshot`
  (reset-on-read) — built as standalone, server-agnostic machinery rather
  than wired into Stage 5's `GroupServer` as the TASKS.md bullet literally
  named: that server is a finished, shipped type built around a fixed
  `NShards`, and bolting a dynamic-shard counter onto it is a structural
  decision Day 4 (real splitting) should make deliberately, not something
  that rides in on a "just add a counter" day. `Snapshot` hands back the
  live counts map directly rather than a defensive copy — safe because the
  same locked call immediately replaces it with a fresh map, a full
  ownership transfer with nothing left to race on. Mutation-checked:
  deleting the reset was caught immediately by the reset-semantics test.
  20/20 clean stage runs and a full clean `go test ./... -race`.
- 2026-10-02 — Stage 6 Day 3 (Hot shard detection) complete: pure
  `DetectHot(loads, shards, HotPolicy)` plus `MergeLoads`. A shard is hot
  when its load strictly exceeds `Factor` x the mean over ALL ring shards
  (idle shards absent from a Snapshot count as zero, otherwise a single
  busy shard among idle ones is its own mean and never flags); optional
  `MinLoad` floor; hottest-first deterministic ordering. Mutation-checked:
  `>=` threshold and mean-over-reported-only were both caught. No polling
  or cluster wiring yet, that's Day 5. Stage run clean under `-race`.
- 2026-10-02 — Stage 6 Day 4 (Splitting a shard, replicated) complete:
  `RingConfig` with pure `SplitConfig`/`MoveRingShard`/`Midpoint`, and
  `RingCtrler`, Stage 5's Ctrler machinery over a ring-config history
  (Init/Split/Move/Query). Parallel type, not a modification — Stage 5's
  `Config` is a fixed array. Both split halves stay on the current owner,
  so a split never changes any key's group (property-tested). Ops are
  validated at apply time with the verdict returned to the proposer,
  closing the check-then-commit gap Stage 5's Move documented; dedup stores
  the verdict so retries of a rejected op aren't told OK. Mutation-checked
  (3 mutants; the retry-verdict one initially survived, test added).
  Stage run clean 15x under `-race`.
