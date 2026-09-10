# Stage 1: Raft Consensus From Scratch

Package: `raft` (import path `github.com/aksharraikanti/distsys-lab/01-raft`)

## Why this stage

Every later stage in this track sits on top of a replicated log. Raft is the
foundation — get it right here and Stage 2 (the KV store) is "just" a state
machine driven by this log.

## Concept notes

_(fill this in as you learn — one section per day, in your own words. The goal
isn't a polished writeup, it's a record of what actually clicked. Good prompts:
what confused you, what the "aha" was, what you'd tell someone else starting
this.)_

### Day 1 — RPC scaffolding
- The RPC shapes (`RequestVoteArgs`/`Reply`, `AppendEntriesArgs`/`Reply`) come straight
  from Figure 2 of the Raft paper — they're not something to invent, they're the
  paper's own field list.
- The key design decision is the `Transport` interface: it decouples "how a node
  reaches a peer" from "what a node does when reached." Two implementations exist —
  `NetTransport` (real TCP via `net/rpc`) and `FakeTransport` (direct in-process
  function calls). This split is what makes Day 12's fault-injection tests possible:
  a fake transport can drop/delay/duplicate a call, a real socket can't be told to
  do that on demand.
- `net/rpc` requires an *exported* type with methods `func(args, reply *T) error` —
  it can't register an interface directly. `raftRPCService` in `net_transport.go` is
  the thin adapter that makes that requirement disappear from the rest of the code.
- Day 1's handlers are intentionally stubs (`RequestVote`/`AppendEntries` always
  refuse) — the test only proves the RPC *round-trips*, not that voting/replication
  logic exists yet. That logic starts Day 3-4.

### Day 2 — Server states
- The three roles (Follower/Candidate/Leader) aren't a free-for-all state machine —
  Raft only allows specific transitions. Codifying which ones are illegal
  (`Follower -> Leader` directly, `Leader -> Candidate`) turned out to be as
  important as the ones that are legal; a state enum with no transition rules is
  just a label, not a state *machine*.
- The subtle one: `BecomeFollower` only resets `votedFor` when the term actually
  advances. A Candidate that loses an election and steps back to Follower *in the
  same term* must remember who it already voted for — otherwise a node could vote
  twice in one term through a state-transition side door, which is exactly the kind
  of bug that looks fine in a demo and breaks safety under partition.
- `BecomeCandidate` increments the term and votes for itself in one atomic step
  (single mutex hold) — if those were two separate locked sections, a concurrent
  RPC could observe a half-updated state (new term, stale vote) that never legally
  exists.
- The Day 2 race test (`TestConcurrentStateAccess`) doesn't have a real election
  timer to race against yet (that's Day 3) — it stands a goroutine that hammers
  `BecomeCandidate()` in for it. The point isn't the specific goroutine, it's
  proving the mutex makes *any* concurrent caller safe, whatever ends up calling
  these methods later.

### Day 3 — Election timeouts
- The *randomization* is the whole point, not an implementation detail. If every
  node used the same fixed timeout, every node would notice a leader failure and
  become a Candidate in the same instant — every election would split every time,
  forever. A random duration per node (in a range) means whoever's timer fires
  first usually gets a clean head start before anyone else even notices.
- The reset channel is deliberately non-blocking (`select { case ch <- struct{}{}: default: }`).
  A blocking reset would mean an RPC handler could stall waiting for the timer
  goroutine to be ready to receive — and a dropped reset is harmless (worst case,
  the current countdown just runs a bit longer), so there's nothing to protect by
  blocking.
- `timer.Stop()` returning `false` means the timer already fired and its value is
  sitting unread in the channel — you have to drain it (`<-timer.C`) before calling
  `Reset`, or the old fire event leaks through on the next loop iteration. This is
  a genuinely easy trap in Go's `time.Timer` API, not specific to Raft.
- Testing a timing-dependent system without flakiness meant polling for the
  condition (`waitFor`) instead of `sleep(exactDuration); assert`. A fixed sleep
  either wastes time being over-cautious or is flaky being too tight — polling
  succeeds the instant the real condition is true, on whatever machine runs it.

### Day 4 — Leader election
- This was the day flagged in advance as likely to run long, and it did — the
  actual RequestVote/AppendEntries *rules* (Day 1) were easy; the part that took
  real thought was the *concurrency* of running an election: firing RPCs to every
  peer at once, then having each reply handler independently decide "does this
  still matter?" before touching shared state.
- The three guard conditions in `startElection`'s per-reply handler
  (`reply.Term > r.currentTerm`, `r.state != Candidate`, `r.currentTerm != term`)
  aren't defensive padding — each one is a real race a slow network can trigger.
  A reply can arrive after the candidate already won on other votes, after it
  lost and became a Follower, or after it gave up and started a *newer* election.
  Any one of those means "this reply belongs to a decision that's already over,"
  and applying it anyway would be a genuine correctness bug, not just untidy code.
- `becomeFollowerLocked`/`becomeCandidateLocked`/`becomeLeaderLocked` exist
  because `startElection`'s reply handler is already holding `r.mu` when it needs
  to transition state — calling the public `BecomeFollower()` etc. from inside
  that handler would deadlock against itself. This is exactly the kind of bug
  the design doc predicted first-timers hit on this day, just one layer removed:
  not "was my term comparison right" but "did I re-lock a mutex I'm already
  holding."
- The log-up-to-date check (`candidateLogIsUpToDateLocked`) is written against
  the *general* rule even though the log is empty until Day 7 — every node's log
  is trivially tied right now, so the check always passes today. Writing the real
  rule now instead of a temporary stub means Day 7 doesn't need to come back and
  rewrite Day 4's voting logic.
- `votes` (the vote-counting integer in `startElection`) is a plain `int`, not an
  atomic — safe here only because every access happens inside a `r.mu.Lock()`
  section already required for the state checks above it. If a future refactor
  ever separates vote-counting from state-checking into two different locked
  sections, that safety argument breaks silently.

### Day 5 — Heartbeats
- The `HeartbeatInterval` constant had been sitting at exactly `10ms` — the same
  value as `ElectionTimeoutMin` — since Day 1, and nothing caught it because
  nothing depended on the *gap* between the two constants until heartbeats
  actually started resetting timers today. Standard Raft guidance wants
  broadcast time well under the election timeout floor (5-10x), so I dropped
  `HeartbeatInterval` to `2ms`. This is the kind of bug that's invisible until
  the exact day it becomes load-bearing — worth remembering for any tunable
  constant introduced before its consumer exists.
- `AppendEntries`'s real logic turned out to be almost a mirror of `RequestVote`'s
  from Day 4: same "become follower if the term is at least current" pattern,
  same reliance on `becomeFollowerLocked`'s built-in "only reset votedFor if the
  term actually advanced" safety. Writing Day 4 and Day 5's handlers back to back
  made a pattern obvious that wasn't obvious reading the paper linearly: both RPCs
  are really "prove you're at least as current as me, and I'll acknowledge you,"
  just with different acknowledgment payloads (a vote vs. a success flag).
- The one-line reason a Candidate steps down on a *same-term* AppendEntries (not
  just a higher-term one) is subtle enough to be worth stating plainly: while an
  election is in flight, it's entirely possible another candidate already won
  *this exact term* and is now sending heartbeats. Ignoring that (only stepping
  down on strictly higher terms) would let two nodes both believe they're leader
  of the same term simultaneously — a real safety violation, not just a
  liveness hiccup.
- `TestHeartbeatsKeepLeaderStable` is the day's real payoff test: Day 4's
  `TestElectionEndToEndViaTimer` only proved a leader *gets elected*; nothing
  stopped a follower from later timing out and deposing it. This test proves
  the missing piece — once heartbeats exist, leadership actually stays put
  through many election-timeout cycles, which is the whole point of heartbeats
  existing at all.

### Day 6 — Election edge cases
- No new production code today — every edge case in this day turned out to
  already be correctly handled by Days 2-5's logic. The day's actual job was
  proving that with tests, not implementing anything new. That's worth
  noticing in itself: getting the *primitives* right early (one-vote-per-term,
  "step down on term >= mine," term-never-decreases) is what makes edge cases
  fall out for free instead of needing special-cased handling later.
- The split-vote test needed the swing voters' votes *pre-committed by directly
  poking `votedFor`* rather than by racing real concurrent `startElection` calls
  against each other — real concurrency would make which candidate a given
  voter favors nondeterministic, and the whole point of this test is proving a
  *specific, guaranteed* split (2-2 in a 4-node cluster) doesn't falsely elect
  anyone. Forcing the scenario deterministically is what makes it a real test
  instead of a test that only sometimes exercises the code path it claims to.
- `TestStaleLeaderStepsDownOnHigherTermReply` found a gap in test coverage, not
  a gap in the code: `sendHeartbeats`'s "step down if a reply reveals a higher
  term" branch existed since Day 5 but nothing had actually exercised it — Day
  5's tests only covered the follower side (rejecting a stale leader), never
  the leader side (a stale leader discovering it's stale). Same logic, opposite
  direction; worth remembering that "the handler is tested" and "every branch
  that handler participates in is tested" aren't the same claim.
- Node restarts are deliberately scoped to *liveness*, not *safety*, and the
  test's doc comment says so explicitly: a restarted node has no memory of its
  prior term or vote (no persistence yet), so in principle it could vote for a
  candidate it "should" remember refusing. That's a real, currently-open safety
  gap — closing it for real is Day 11's entire job, not something to
  half-solve here with an ad hoc workaround.

### Day 7 — Log entries
- `Propose` only touches the *leader's own* log — nothing about replicating the
  entry to followers or knowing when it's safely committed lives here. That
  split matters: "a client asked for this" (Day 7), "the cluster has a copy of
  it" (Day 8), and "it's safe to apply" (Day 9) are three genuinely different
  guarantees, and conflating them into one method would make it impossible to
  reason about what's actually been promised at any given point.
- `AppendEntries`'s new append logic (`r.log = append(r.log[:args.PrevLogIndex], args.Entries...)`)
  is deliberately naive — it *trusts* PrevLogIndex instead of verifying the
  follower's log actually agrees with the leader at that position first. That's
  not an oversight; it's explicitly Day 10's job ("log consistency check"),
  named as such in the Raft paper as a separate concern from "how do entries
  get appended at all." Building it now would mean re-deriving Day 10's logic
  early, out of order, from a position with less context than Day 8 and 9 will
  provide.
- Nothing currently calls `AppendEntries` with real (non-empty) entries —
  `sendHeartbeats` still only ever sends empty ones. That makes today's new
  append logic dead code from the running system's point of view, exercised
  only by direct test calls. That's fine and expected for a day whose job is
  "prove the capability exists," not "wire it into the leader's send loop" —
  that wiring is exactly what Day 8 adds.
- The overwrite test (`TestAppendEntriesOverwritesFromPrevLogIndex`) is the one
  that actually exercises `r.log[:args.PrevLogIndex]`'s truncation behavior —
  the append-only test alone (`TestAppendEntriesAppendsRealEntries`, using
  `PrevLogIndex: 0` on an empty log) wouldn't catch a bug in the "throw away
  anything after PrevLogIndex" half of that one line.

### Day 2 — Server states
-

_(continue per day)_

## Reference material

- Raft paper: "In Search of an Understandable Consensus Algorithm" (Ongaro & Ousterhout) — https://raft.github.io/raft.pdf
- The Secret Lives of Data (visual walkthrough) — http://thesecretlivesofdata.com/raft/
- MIT 6.5840 (formerly 6.824) Lab 3 (Raft) — this stage's day-by-day breakdown closely follows this lab's structure, adapted to a solo daily-cadence pace.

## Status

See [TASKS.md](TASKS.md) for the day-by-day checklist and [../PROGRESS.md](../PROGRESS.md) for where things stand right now.
