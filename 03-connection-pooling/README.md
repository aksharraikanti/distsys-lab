# Stage 3: Connection Pooling Layer

Package: `pool` (import path `github.com/aksharraikanti/distsys-lab/03-connection-pooling`)

## Why this stage

Every client in Stage 2 has been `Clerk` calling `KVServer` methods directly,
in-process — no socket, no dial cost, no connection to manage at all. That was
the right simplification while the point was proving linearizability and fault
tolerance, but it means this whole track has never actually paid the cost real
clients pay to reach a real server. This stage puts a real network boundary
back in (plain `net/rpc` over TCP, the same primitive 01-raft's own
`NetTransport` already uses for inter-node traffic) and then asks the question
pooling exists to answer: dialing a fresh connection per request is slow and
wasteful under load, so how do you reuse a bounded set of connections safely —
across concurrent callers, across a backing node crashing and coming back,
without leaking connections or deadlocking under backpressure?

## Concept notes

_(fill this in as you learn — one section per day, in your own words.)_

_(continue per day)_

## Reference material

- MIT 6.5840's own `labrpc` package — 01-raft's `NetTransport`/`FakeTransport`
  split already mirrors this; Stage 3's real-vs-simulated network split
  follows the same shape.
- `database/sql`'s own connection pool (`DB.SetMaxOpenConns`,
  `SetMaxIdleConns`, `SetConnMaxIdleTime`) — a widely-used, battle-tested
  pooling API worth reading before designing this stage's own, even though
  this stage builds its own from scratch rather than importing it.

## Status

See [TASKS.md](TASKS.md) for the day-by-day checklist and [../PROGRESS.md](../PROGRESS.md) for where things stand right now.
