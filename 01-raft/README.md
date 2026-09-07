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
-

_(continue per day)_

## Reference material

- Raft paper: "In Search of an Understandable Consensus Algorithm" (Ongaro & Ousterhout) — https://raft.github.io/raft.pdf
- The Secret Lives of Data (visual walkthrough) — http://thesecretlivesofdata.com/raft/
- MIT 6.5840 (formerly 6.824) Lab 3 (Raft) — this stage's day-by-day breakdown closely follows this lab's structure, adapted to a solo daily-cadence pace.

## Status

See [TASKS.md](TASKS.md) for the day-by-day checklist and [../PROGRESS.md](../PROGRESS.md) for where things stand right now.
