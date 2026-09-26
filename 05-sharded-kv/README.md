# Stage 5: Sharded KV Store

Package: `shardkv` (import path `github.com/aksharraikanti/distsys-lab/05-sharded-kv`)

## Why this stage

Stages 2-4 made one replicated KV store correct, reachable, and fast to read.
But it's still one Raft group: every write goes through one leader, and the
whole dataset must fit on one machine. Sharding is how a KV store gets past
that — split the keyspace, give each piece to its own replicated group.

Routing a key to the right group is the easy part. The interesting problem is
that the assignment changes — groups join, leave, load shifts — and a shard's
data has to move between groups WHILE clients keep using it. Everything hard in
this stage is a variation on one question: at every instant, exactly one group
must be answerable for each key, and that group must have every write ever
acknowledged for it, exactly once.

## Concept notes

_(fill this in as you learn — one section per day, in your own words.)_

_(continue per day)_

## Reference material

- MIT 6.5840 Lab 4 (Sharded Key/Value Service) — this stage's structure follows
  it, adapted to a solo daily pace, the same way Stage 2 followed Lab 3.
- Wing & Gong, "Testing and Verifying Concurrent Objects" — the linearizability
  checking algorithm Day 7's history checker is a simplified version of.

## Status

See [TASKS.md](TASKS.md) for the day-by-day checklist and [../PROGRESS.md](../PROGRESS.md) for where things stand right now.
