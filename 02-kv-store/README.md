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

_(continue per day)_

## Reference material

- MIT 6.5840 Lab 3 (Fault-tolerant Key/Value Service) — this stage's structure
  closely follows this lab, adapted to a solo daily-cadence pace, same as
  Stage 1 followed Lab 2 (Raft).
- Stage 1's [`01-raft/persist.go`](../01-raft/persist.go) — the `gob.Register`
  note left there specifically for this stage.

## Status

See [TASKS.md](TASKS.md) for the day-by-day checklist and [../PROGRESS.md](../PROGRESS.md) for where things stand right now.
