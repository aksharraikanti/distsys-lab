# Progress

Current stage: **01-raft**
Current day: **Day 5 — Heartbeats** (next up)
Status: Day 4 complete

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
