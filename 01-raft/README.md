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

### Day 2 — Server states
-

_(continue per day)_

## Reference material

- Raft paper: "In Search of an Understandable Consensus Algorithm" (Ongaro & Ousterhout) — https://raft.github.io/raft.pdf
- The Secret Lives of Data (visual walkthrough) — http://thesecretlivesofdata.com/raft/
- MIT 6.5840 (formerly 6.824) Lab 3 (Raft) — this stage's day-by-day breakdown closely follows this lab's structure, adapted to a solo daily-cadence pace.

## Status

See [TASKS.md](TASKS.md) for the day-by-day checklist and [../PROGRESS.md](../PROGRESS.md) for where things stand right now.
