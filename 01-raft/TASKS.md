# Stage 1 Tasks: Raft Consensus

Each day: read/learn the concept first, implement it, write a quick test, commit.
Check a box only once it's implemented AND tested — "read about it" isn't done.

- [ ] **Day 1 — RPC scaffolding.** Define the `RequestVote` and `AppendEntries` RPC
      structs, a bare `Raft` struct holding server state, and a pluggable transport
      interface (real net/rpc for normal runs, an in-process fake transport for later
      fault-injection tests — this is what makes Day 12 tractable). Get 3 in-process
      mock nodes talking over it. No election/replication logic yet.
- [ ] **Day 2 — Server states.** Implement the Follower/Candidate/Leader state enum
      and the transition rules between them. No elections triggered yet — just prove
      the state machine transitions correctly under manual calls.
- [ ] **Day 3 — Election timeouts.** Randomized election timeout per node; trigger a
      Follower → Candidate transition when no heartbeat arrives in time.
- [ ] **Day 4 — Leader election.** Implement `RequestVote` handling and vote counting.
      Get a single leader elected among 3 nodes with no faults.
- [ ] **Day 5 — Heartbeats.** Leader sends periodic empty `AppendEntries` as
      heartbeats; followers reset their election timer on receipt.
- [ ] **Day 6 — Election edge cases.** Split votes, term numbers, stale-leader
      rejection. Test with simulated node restarts.
- [ ] **Day 7 — Log entries.** Extend `AppendEntries` to carry real log entries;
      leader appends to its own log on a client request.
- [ ] **Day 8 — Log replication.** Leader replicates entries to followers, tracks
      `matchIndex`/`nextIndex`, retries on failure.
- [ ] **Day 9 — Commit rule.** Leader advances `commitIndex` once a majority has
      replicated an entry; followers apply committed entries to a state machine.
- [ ] **Day 10 — Log consistency check.** Implement the `AppendEntries` consistency
      check (prevLogIndex/prevLogTerm) so followers reject/truncate divergent logs.
- [ ] **Day 11 — Persistence.** Persist term, vote, and log to disk so a crashed node
      recovers correctly on restart.
- [ ] **Day 12(-13) — Fault injection tests.** Using the fake transport from Day 1,
      simulate leader crashes, network partitions, and follower restarts; verify the
      cluster always converges to a single consistent log. This is the hardest day in
      the stage — budget for it spilling into a second day.

## Done means

- `go test ./01-raft/... -race` passes, including the Day 12 fault-injection suite.
- Stage 1 README's concept notes are filled in for every day.
- `PROGRESS.md` updated, Stage 2 scoped next.
