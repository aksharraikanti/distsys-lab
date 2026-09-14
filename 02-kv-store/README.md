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

_(continue per day)_

## Reference material

- MIT 6.5840 Lab 3 (Fault-tolerant Key/Value Service) — this stage's structure
  closely follows this lab, adapted to a solo daily-cadence pace, same as
  Stage 1 followed Lab 2 (Raft).
- Stage 1's [`01-raft/persist.go`](../01-raft/persist.go) — the `gob.Register`
  note left there specifically for this stage.

## Status

See [TASKS.md](TASKS.md) for the day-by-day checklist and [../PROGRESS.md](../PROGRESS.md) for where things stand right now.
