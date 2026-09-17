# Stage 2 Tasks: Fault-Tolerant KV Store on Raft

Builds directly on 01-raft. Each day: read/learn the concept first, implement
it, write a quick test, commit. Check a box only once it's implemented AND
tested — "read about it" isn't done.

- [x] **Day 1 — Apply-loop scaffolding.** Define `Op` (the state-machine
      command — Put/Append, with Key/Value), a `KVServer` wrapping a
      `*raft.Raft`, and the apply loop that consumes `rf.ApplyCh` and builds
      up an in-memory `map[string]string`. `Op` gets `gob.Register`'d in this
      package's own `init()` — the exact forward-pointer Stage 1's Day 11 left
      for this stage. No client-facing Get/Put RPC serving yet (Day 2), no
      duplicate request detection (Day 3). `Get` is a direct, non-Raft-routed
      local read — proving the apply loop is correct is this day's only job;
      making reads linearizable is explicitly later.
- [x] **Day 2 — Client-facing RPC handlers.** `Get`/`PutAppend` RPC types and
      handlers: a client calls a server, the server `Propose`s the
      corresponding `Op`, waits for that specific log index to actually
      commit (a per-index completion mechanism — not just "wait for
      commitIndex to advance," since a proposed entry can be overwritten by a
      later leader before it ever commits), and replies with the result or an
      explicit "not leader" error so the client knows to retry elsewhere.
- [x] **Day 3 — Duplicate request detection.** Client ID + monotonic sequence
      number per client, tracked in the state machine itself (so it survives
      snapshots later). A client retries a request whenever it can't tell if
      its last one succeeded (timeout, leader change) — without this, a
      retried Append would silently double-apply.
- [x] **Day 4 — Leader-change correctness.** A command can be `Propose`d,
      start replicating, and then never commit because this node loses
      leadership before a majority confirms it (a fresher leader's log
      overwrites the entry). The waiting client must detect this (e.g. the
      term at that index changed, or this node stopped being leader) and
      report failure/retry — never hang forever, and never falsely report
      success for an entry that got silently discarded.
- [x] **Day 5 — Concurrent client stress test.** Many simulated clients
      hammering Get/Put/Append concurrently, through the same fault injection
      Stage 1 Day 12 built (leader crashes, partitions). The invariant under
      test: every acknowledged write is durably visible to every subsequent
      read, and no acknowledged write is ever lost — this is where the whole
      stage either holds together or doesn't. Turned up three real bugs along
      the way: a freshly-elected leader serving stale reads until something in
      its own term commits (fixed with a Raft §8 no-op-on-election), a
      goroutine leak in `KVServer`'s loops that caused cross-test flakiness
      (fixed with `Stop()`), and a `Clerk` ClientID collision from
      time-seeded `math/rand` (fixed by drawing from `crypto/rand` instead).
- [x] **Day 6 — Snapshotting.** Once the Raft log grows past a size
      threshold, `KVServer` serializes its current map into a snapshot and
      tells Raft it can discard log entries up to that point — otherwise the
      log grows forever, which Stage 1 never had to solve since nothing was
      consuming committed entries into compactable state. Required extending
      01-raft itself (not just the KV package): every absolute log index now
      has to translate through `lastIncludedIndex`, since `r.log` only holds
      entries after the most recent compaction, and `Persister` gained a
      second, independently-stored blob for the opaque snapshot bytes
      alongside the existing term/votedFor/log.
- [ ] **Day 7 — InstallSnapshot RPC.** A new Raft RPC (extending 01-raft) for
      the case Day 6 creates: a follower that's fallen far enough behind that
      the leader has already discarded the log entries it needs. Instead of
      rejecting forever, the leader sends a full snapshot instead — the
      follower installs it and catches up from there via normal replication.
- [ ] **Day 8 — Full integration.** Fault injection (crashes, partitions,
      restarts) combined with concurrent clients AND snapshotting all running
      at once — the fullest test this stage can produce, proving the pieces
      built across Days 1-7 actually compose.

## Done means

- `go test ./02-kv-store/... -race` passes, including the Day 8 integration suite.
- Stage 2 README's concept notes are filled in for every day.
- `PROGRESS.md` updated, Stage 3 scoped next.
