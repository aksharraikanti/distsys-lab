# Stage 2: Fault-Tolerant KV Store on Raft

Package: `kvstore` (import path `github.com/aksharraikanti/distsys-lab/02-kv-store`)

## Why this stage

Stage 1 built a correct replicated log. That's necessary but not the point —
nobody wants a log, they want a durable key-value store. This stage is where
`ApplyCh` finally gets a real consumer, and where "the log is correct" turns
into "the store is correct," which is a genuinely different, harder claim
(linearizability, not just log consistency).

## Concept notes

_(fill this in as you learn — one section per day, in your own words.)_

### Day 1 — Apply-loop scaffolding
- `Op` deliberately doesn't have a `"Get"` variant. Get doesn't mutate state,
  so there's nothing for it to log — the apply loop only needs to know about
  the two things that actually change the map. Whether reads should ALSO go
  through the log (for linearizability, so a partitioned-away leader can't
  serve a stale read) is a real, open question this stage will have to answer
  later, not something to guess at now.
- The convergence test (`TestKVStoreConvergesAcrossCluster`) is the one that
  actually proves the point of this whole stage, not the single-node tests.
  Three independent `KVServer`s, each with its own goroutine consuming its
  own `ApplyCh`, reach the identical final map with zero coordination between
  them — the *only* thing making that true is that Raft guarantees every
  node's log commits entries in the same order. If that guarantee ever broke,
  this is the test that would catch it, not the single-node ones.
- `TestOpGobRegistrationRoundTrips` exists because Stage 1's Day 11 explicitly
  left a forward-pointer for this exact test — "Stage 2's KV store will need
  its own registration for whatever concrete Command type it introduces." It
  would have been easy to add `gob.Register(Op{})` and just trust it works;
  writing the test that actually forces a decode of a persisted, Op-carrying
  log is what turns that trust into a verified claim.
- `Get`'s doc comment is doing real work, not just narrating: it says
  explicitly that this read is NOT linearizable yet, and names exactly why
  (no Raft routing, no leader check). Future-me reading this file mid-Stage-2
  needs that warning readily visible, not buried in a TASKS.md line — a
  reader who only opens `server.go` should still learn the limitation.

### Day 2 — Client-facing RPC handlers
- The per-index notify-channel pattern (`notifyChans map[int]chan Op`,
  buffered(1)) exists to answer a question that "just wait for commitIndex to
  advance" can't: WHICH command actually landed at the index this handler
  cares about. Those are different questions — a later leader's entry can
  legitimately occupy the same index this node proposed to. Comparing the
  received `Op` against the proposed one (`applied != op`) is what turns
  "something committed at index N" into "MY thing committed at index N."
  Day 3's client-id/sequence-number work will make this comparison far more
  precise than plain struct equality, but the mechanism — wait on this index,
  compare what shows up — doesn't change.
- Writing `TestPutAppendDetectsSupersededProposal` surfaced a real bug in the
  TEST, not the production code, and it's worth remembering the shape of it:
  both nodes start at term 0, so giving node1 a single `BecomeCandidate()`
  call only TIES node0's term (both land on 1) rather than exceeding it. Which
  node's heartbeat happened to reach the other first — a coin flip — decided
  who superseded whom, so the test passed roughly half the time for the wrong
  reason. The fix was two `BecomeCandidate()` calls on node1 to deterministically
  reach term 2. General lesson: when a test needs a "definitely higher term,"
  derive it from the OTHER side's actual value, don't assume a fixed number of
  transitions gets you there — assuming this caused about 20 minutes of
  confused debugging before the actual bug (a race in the TEST, not the
  production code) became clear.
- Stress-testing this day surfaced something outside its own scope entirely:
  running `go test ./...` (both packages) under real CPU contention exposed a
  ~13% flaky failure in Stage 1's `TestHeartbeatsKeepLeaderStable` — a
  pre-existing test-timing-margin issue (unrelated to anything Day 2 touched)
  that a growing test suite now surfaces more readily. Flagged as a separate
  follow-up rather than folded into this PR — it's not Day 2's bug to fix, and
  bundling an unrelated Stage 1 fix into a Stage 2 PR would muddy both.

### Day 3 — Duplicate request detection
- Adding `ClientID`/`SeqNum` to `Op` broke roughly a dozen existing test call
  sites in a way that was easy to predict once I thought about it but easy to
  miss otherwise: Go's zero value for an unset `SeqNum` is `0`, and
  `duplicateTable[clientID]` also defaults to `0` for a never-seen client — so
  `0 > 0` is false, meaning every OLD `Op{}`/`PutAppendArgs{}` literal that
  didn't set `SeqNum` would have its effect silently skipped by the new dedup
  check. The fix was updating every call site with a real `SeqNum` starting at
  1, not adding a `SeqNum == 0` bypass — a bypass would have been a genuine
  loophole (any buggy or malicious client could send `SeqNum: 0` forever and
  dodge dedup entirely), and real client libraries this project is modeled on
  never allow that escape hatch either.
- The dedup table lives in the applyLoop/state machine, not the RPC handler
  layer, and that placement is load-bearing, not a style choice: every replica
  applies the identical committed log in the identical order, so every replica
  computes the SAME answer to "have I already applied this SeqNum" — the
  dedup decision itself is replicated and consistent, not a per-node guess
  that could disagree across the cluster. Putting it in the RPC layer instead
  would have made "is this a duplicate" a question each node answers
  independently from its own possibly-stale view, which defeats the purpose.
- `applyLoop` still notifies the index's waiter even when the dedup check
  skips the actual mutation — this is the detail that makes retries behave
  correctly from the CLIENT's point of view. The retry's own RPC call
  registered its own notify channel at its own (new) log index; that call
  still needs its own reply, even though the underlying Put/Append effect
  already happened via the original attempt. Skipping the notify here would
  leave the retrying caller hanging until `commitTimeout`, reporting a
  timeout for an operation that had actually already succeeded.
- `TestStaleRetryAfterNewerRequestSuppressed` is the test that actually
  distinguishes a correct implementation from a plausible-looking wrong one:
  using `!=` or `==` instead of strict `>` in the dedup comparison would pass
  every OTHER test in this file, since none of them exercise a stale request
  arriving AFTER a newer one from the same client already applied. Only `>`
  correctly treats "older than what I've already seen" as still a duplicate,
  regardless of whether its SeqNum happens to differ from the current
  high-water mark.

### Day 4 — Leader-change correctness
- This day turned out to be about a gap Day 2 left half-closed, not a whole
  new mechanism. Day 2's `applied != op` check already correctly detects
  supersession — but only IF something new eventually lands at that exact log
  index. If this node loses leadership and NOTHING ever writes to that index
  again (a real possibility — a future leader has no obligation to ever touch
  that specific slot), the notify channel simply never fires, and the only
  thing Day 2 had to fall back on was the full `commitTimeout`. That's correct
  but slow. The actual improvement here isn't a new correctness guarantee, it's
  a MUCH faster way to reach the same, already-correct conclusion.
- `leaderCheckInterval`'s poll checks BOTH `Term() != term` and `State() !=
  Leader`, not just one — documented explicitly as belt-and-suspenders around
  the same underlying fact rather than two independent checks. As long as this
  node has stayed Leader continuously, in the SAME term, since it Proposed,
  nothing else could have written to that index without its own consent — the
  term changing (or ceasing to be Leader) is definitionally what a step-down
  looks like from the inside. Checking both costs nothing and reads clearly;
  checking only one would be equally correct but less obviously so on a
  first read.
- `TestPutAppendSucceedsWhenLeadershipNeverLost` exists specifically because
  adding a polling loop to an otherwise event-driven wait is exactly the kind
  of change that can introduce a race nobody intended — a poll firing at the
  "wrong" moment relative to a legitimate, in-flight commit could in principle
  misfire into a false `ErrWrongLeader`. This test is the guard against that:
  proving the happy path still reliably returns `OK`, not just that the new
  unhappy path works.

### Day 5 — Concurrent client stress test
- Writing `Clerk` surfaced a design point worth naming explicitly: a single
  `Clerk` is NOT safe for concurrent use — its `seqNum` counter and
  `lastKnown` server hint aren't synchronized. That's fine (even correct) for
  this stage's model, where each simulated client gets its OWN `Clerk`
  instance, but it would silently corrupt the ClientID/SeqNum contract Day 3
  depends on if two goroutines ever shared one. Worth a comment on the type,
  not just something to remember.
- The headline invariant — every acknowledged write durably visible, no
  acknowledged write ever lost — turned up three separate, real bugs, each
  discovered because concurrency + fault injection finally exercised code
  paths the single-threaded Day 1-4 tests never reached:
  1. **Stale reads from a freshly-elected leader.** Day 9's Figure 8 rule (a
     new leader can't mark any older-term entry committed until something in
     its OWN term reaches a majority) means a brand-new leader's view of
     "what's committed" can lag behind what a majority of the cluster
     actually holds — even though its LOG already has every one of those
     entries, thanks to the up-to-date voting rule. `Get` would serve
     `ErrNoKey` for a key the cluster had already durably written. Fixed with
     the exact technique the Raft paper names for this (§8): `noopLoop`
     proposes a no-op entry once per newly-observed leadership term, and once
     THAT commits, Figure 8 transitively reconfirms everything that came
     before it.
  2. **A goroutine leak that looked like unrelated flakiness.** `applyLoop`
     and `noopLoop` had no shutdown mechanism, so every earlier test's
     orphaned `KVServer`s — especially ones whose Raft node was still Leader
     when the test ended — leaked those goroutines for the rest of the whole
     test binary's process lifetime. An orphaned, ever-ticking `noopLoop`
     kept calling `Propose` against a stale Raft instance, accumulating
     scheduler contention across the whole run. Same underlying lesson as the
     Stage 1 flaky-test fix, just in a new spot: added `stopCh`/`stopOnce`/
     `Stop()` to `KVServer`, the same idempotent-close shape
     `raft.Raft.StopElectionTimer` already used, and called it everywhere a
     test constructs a `KVServer`.
  3. **`Clerk` ClientID collisions.** The subtlest of the three, and the one
     that took the longest to pin down: `NewClerk` seeded `math/rand` from
     `time.Now().UnixNano()`. Launching several client goroutines back to
     back (exactly what the stress test does) can construct two `Clerk`s
     within the same wall-clock nanosecond, which seeds them identically and
     hands out the SAME "random" ClientID to two genuinely different
     clients. That collision corrupts `duplicateTable`'s per-ClientID
     tracking: one client's write gets treated as an already-seen duplicate
     of the other's and its mutation is silently skipped — yet `PutAppend`
     still reports `OK`, because the notify channel fires unconditionally on
     the proposed op matching what came back, regardless of whether the
     dedup check actually let the mutation through. That's what made it look
     exactly like data loss with no error anywhere: a fully successful-looking
     RPC round trip whose effect never happened. Root-caused by tracing one
     specific ClientID through a debug log and finding it used by Propose
     calls for two different clients' keys. Fixed by drawing ClientID from
     `crypto/rand` instead of a wall-clock-seeded `math/rand` source — cheap,
     and removes the whole class of seed-collision risk rather than just
     widening the window.

### Day 6 — Snapshotting
- This day turned out to live mostly in `01-raft`, not `02-kv-store` —
  the KV-store side (`snapshotLocked`, the size check in `applyLoop`,
  restoring from `rf.ReadSnapshot()` in `NewKVServer`) is almost
  incidental compared to what compaction demands of Raft itself. Every
  place that indexed `r.log` directly (`AppendEntries`,
  `advanceCommitIndexLocked`, `applyPending`, `replicateToPeer`,
  `lastLogInfoLocked`, `Propose`) was written, across Stage 1, on the
  unstated assumption that `r.log`'s physical position and a paper-style
  absolute index are the same number. Compaction breaks that assumption
  outright — `r.log` only holds entries AFTER `lastIncludedIndex` once
  `Snapshot` has trimmed a prefix off it — so every one of those sites
  needed a real translation (`physicalIndexLocked`/`termAtLocked`), not
  a patch. Worth remembering for future "just bolt this feature on"
  estimates: a change that sounds additive can still touch nearly every
  file in a package if enough existing code was written against an
  assumption the new feature invalidates.
- `Persister` grew a second, INDEPENDENT pair of methods
  (`SaveStateAndSnapshot`/`ReadSnapshot`) rather than folding snapshot
  bytes into the existing `SaveState` blob. Two reasons: snapshots can
  be large and change on a different rhythm than term/votedFor/log
  (every mutation), and — more load-bearing — `SaveStateAndSnapshot`
  writes snapshot BEFORE state, deliberately, because state is what
  "commits" the compaction (it carries `LastIncludedIndex/Term`, the
  claim that a prefix is reconstructible from the snapshot instead of
  the log). A crash between the two writes is only safe in that order:
  writing state first and crashing before snapshot lands would leave a
  claim on disk with nothing backing it up — real, unrecoverable data
  loss on the next restore. Writing snapshot first just leaves a stray,
  harmless file if a crash lands in between.
- The size-check placement in `applyLoop` — inside the same
  already-held `kv.mu` critical section that just applied an entry,
  checked after EVERY entry rather than on a separate timer — wasn't
  the obvious choice going in, but it turned out to be the simplest
  correct one: `RaftStateSize` only grows one entry at a time, so
  there's no way to overshoot a threshold between checks, and the lock
  is already held with exactly the consistent `store`/`duplicateTable`
  view `snapshotLocked` needs to serialize. A separate ticker would
  have needed its own re-locking and its own reasoning about "what if a
  entry is applied between now and my next tick" for no actual benefit.
- `kvSnapshot` bundles `store` AND `duplicateTable` into one encoded
  blob, not two separate `Snapshot` calls or two fields serialized
  independently. They have to be restored as of the exact same log
  index — restoring one without the other reopens exactly the class of
  bug Day 3 closed: a request whose effect landed right at the
  snapshot's boundary could either double-apply (dedup entry missing)
  or get wrongly treated as already-seen (store entry missing) after a
  restart, depending on which half survived and which didn't.
- Writing `TestKVServerNeverSnapshotsWhenDisabled` as the explicit
  control for `TestKVServerSnapshotsWhenRaftStateExceedsThreshold` felt
  like overkill at first — of course `maxRaftState: -1` disables
  snapshotting, it's a straightforward early-return. But without a test
  actually asserting "and this state stays large, unbounded," nothing
  in this package would catch a future change that accidentally made
  `-1` behave like "snapshot immediately" instead of "never" — the
  positive test alone can't distinguish "snapshotting is disabled" from
  "snapshotting is so aggressive it also happens to pass."

### Day 7 — InstallSnapshot RPC
- The single most important thing this day found wasn't a missing
  feature, it was a race hiding in Day 6's own design: `InstallSnapshot`
  runs on whatever goroutine the RPC arrives on (the leader's
  `replicateToPeer` goroutine, via `FakeTransport`'s synchronous
  dispatch), which is a DIFFERENT goroutine from the one
  `RunApplyLoop`/`applyPending` already uses to send on `ApplyCh`. Two
  independent senders on one channel means the ORDER two messages land
  in isn't guaranteed to match the order they were logically produced
  in — a regular entry committed just before a newer InstallSnapshot
  could, in principle, be delivered to the state machine AFTER that
  snapshot already replaced it, which would silently regress state.
  The fix: `InstallSnapshot` doesn't send on `ApplyCh` at all — it just
  queues the message (`pendingSnapshot`), and `RunApplyLoop`'s own
  goroutine delivers it (checking for one before every regular
  `applyPending` call). Restoring "exactly one sender" is what makes
  the delivery order provably correct again, not just usually correct.
  This is a general lesson worth remembering past this project: adding
  a second producer to an existing single-producer channel is a design
  change, not just a convenience — the invariant that made the old code
  correct ("only one thing ever sends here") has to be re-established
  somehow, or checked to see if it was ever actually needed.
- The SAME "in-memory node without a persister should still work"
  principle that's threaded through this whole project (most tests use
  `raft.NewRaft`, not `NewRaftWithPersister`) almost broke silently
  here: my first pass at `sendInstallSnapshot` read snapshot bytes via
  `persister.ReadSnapshot()`, which is `nil`/empty for the common
  persister-less case — meaning a leader that had genuinely compacted
  its log (Snapshot doesn't require a persister; it always trims
  `r.log`) would send an EMPTY snapshot to a lagging follower, silently
  losing every bit of state that prefix represented. Fixed by giving
  Raft its own in-memory `snapshotData` field, independent of whether a
  persister is attached — a persister governs whether a snapshot
  survives a RESTART, not whether a live, running node can hand its
  current snapshot to a peer over RPC, and conflating the two was the
  actual bug.
- Writing the real 3-node, fault-injection-driven test
  (`TestKVServerCatchesUpLaggingFollowerViaInstallSnapshot`) — not just
  the direct, single-RPC unit tests — is what surfaced a SECOND latent
  bug: `Snapshot`'s doc comment always claimed "the state machine has
  already applied through index" as its precondition, but the code
  never actually checked it, only that `index` was within the log's
  bounds. In every unit test I'd written by hand, that held by
  construction; in a real cluster under real timing, `commitIndex`
  reaching an index doesn't mean `applyPending`'s ticker has caught up
  to it yet — a caller (even the test harness itself, standing in for a
  buggy real caller) snapshotting a moment too early would silently
  corrupt the log, discarding entries nothing had consumed. Added the
  missing `index > lastApplied` check so this fails loudly instead.
  Lesson: a doc comment describing a precondition is a promise to
  readers, not a substitute for the code actually enforcing it — and
  the gap between "passes every test I thought to write" and "correct"
  is exactly what a broader, more realistic test (real cluster, real
  timing, real faults) exists to find.

### Day 8 — Full integration
- Splitting this into TWO tests, rather than one that tried to cover
  everything at once, turned out to be the right call rather than a
  compromise. `KVServer`'s client-facing methods are called directly by
  `Clerk` — there's no RPC boundary in this stage, by design (Stage 3 is
  what adds a real network boundary) — which means a `Clerk` holding a
  frozen `[]*KVServer` has no way to observe a mid-test object swap.
  "Crash via unreachability" (Day 5/Day 12's `Unregister`/`Register` and
  `Partition`/`Heal`) keeps every object's identity alive the whole
  time, so it composes fine with a single long-running cluster and
  concurrent `Clerk`s. A REAL restart — discarding and reconstructing
  Raft+KVServer from persisted state — fundamentally can't, without
  either inventing a live-object-swap mechanism inside `KVServer` purely
  to satisfy one test, or accepting that a fresh `Clerk` after a full
  restart is what a real client would do anyway (reconnect after an
  outage, not keep silently retrying against a socket that no longer
  points at anything). Chose the second: two tests, not one clever one.
- The whole-cluster restart test's first run failed — the very last
  fragment of the very last key was missing right after restart, with
  everything else correct. Not data loss: every node's OWN `CommitIndex`
  is volatile, reset on restart (correctly — the Raft paper never
  persists it), and a freshly re-elected leader has to re-confirm its
  commitIndex via real `AppendEntries` replies reaching a majority again
  in its own new term before it catches back up to what was ALREADY
  committed pre-crash — `noopLoop` (Day 5) is what drives that
  re-confirmation, and it just hadn't finished yet the instant the test
  queried `Get`. Same mechanism Day 5's own doc comments already named
  ("a freshly-elected leader hasn't confirmed its own older entries
  yet"), observed for the first time in a whole-cluster-restart
  context rather than a single-leader-failover one. Fixed the test by
  waiting for every restarted node's `CommitIndex` to reach the known
  pre-crash value before trusting any `Get` — not by changing production
  code, since production code was already correct; the test was just
  querying before the system's own documented catch-up window closed.
- Forcing `maxRaftState` low enough that the concurrent-clients-plus-
  faults test ACTUALLY snapshots (checked explicitly at the end, not
  just hoped for) is what finally exercises `InstallSnapshot` under
  real concurrent load and real partition timing, rather than only the
  hand-built single-fault scenario Day 7's own test constructed. This
  is the version of the invariant that matters most: not "does
  InstallSnapshot work in isolation," but "does the whole system — log
  replication, commit rule, snapshotting, InstallSnapshot, duplicate
  detection, and client retries — still hold together when every piece
  is firing at once, which is the only way any of it will ever actually
  run in practice."

### Post-completion fix — reads gated on the leader's own-term no-op
Found by Stage 3 Day 6's load test, fixed in Stage 3's PR. Day 5's
`noopLoop` proposed a no-op on election but `Get` never waited for it to
apply, so a just-elected leader could serve a read missing the last write
its predecessor had acknowledged (Raft §8: a leader must apply an entry
from its own term before serving reads). `Get` now returns `ErrWrongLeader`
until `noopAppliedTerm` equals the current term; clients already retry that.
Still not fully linearizable — a silently partitioned old leader can answer
from a stale store; that needs read-index or leases.

_(continue per day)_

## Reference material

- MIT 6.5840 Lab 3 (Fault-tolerant Key/Value Service) — this stage's structure
  closely follows this lab, adapted to a solo daily-cadence pace, same as
  Stage 1 followed Lab 2 (Raft).
- Stage 1's [`01-raft/persist.go`](../01-raft/persist.go) — the `gob.Register`
  note left there specifically for this stage.

## Status

See [TASKS.md](TASKS.md) for the day-by-day checklist and [../PROGRESS.md](../PROGRESS.md) for where things stand right now.
