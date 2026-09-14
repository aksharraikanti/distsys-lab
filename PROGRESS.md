# Progress

Current stage: **02-kv-store**
Current day: **Day 3 — Duplicate request detection** (next up)
Status: Stage 1 (Raft consensus from scratch) complete, all 12 days.
Stage 2 (Fault-tolerant KV store on Raft) scoped into 8 days, Days 1-2 complete.

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
