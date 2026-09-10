# Progress

Current stage: **01-raft**
Current day: **Day 10 — Log consistency check** (next up)
Status: Day 9 complete

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
