# Progress

Current stage: **01-raft**
Current day: **Day 3 — Election timeouts** (next up)
Status: Day 2 complete

## Log

- 2026-09-07 — Repo scaffolded via `/office-hours`. Stage 1 broken into 12 days. Not yet started.
- 2026-09-07 — Day 1 (RPC scaffolding) complete: RPC types, `Transport` interface,
  `FakeTransport` (in-process) and `NetTransport` (real net/rpc over TCP) both
  implemented, 3-node round-trip tests passing under `-race`.
- 2026-09-08 — Day 2 (Server states) complete: Follower/Candidate/Leader state
  machine with enforced transition rules, mutex-protected shared state, and a
  concurrent race test (RPC handlers + simulated election-timer firing at the
  same instance) clean under `go test -race`.
