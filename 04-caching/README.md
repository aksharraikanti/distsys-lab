# Stage 4: Caching Layer

Package: `cache` (import path `github.com/aksharraikanti/distsys-lab/04-caching`)

## Why this stage

Stage 3's benchmarks already contain the argument for this stage. A pooled
`Get` costs tens of microseconds; a `Put`/`Append` costs milliseconds, because
a write has to replicate through Raft. Reads are the cheap path, but they're
still a network round trip to a node that might be the only leader — and reads
are usually the vast majority of traffic. A cache answers repeated reads from
local memory without touching the network at all.

The price is that the cache is now a second copy of the truth, and every hard
problem in this stage is some form of "what if the copy is wrong": it went
stale because someone else wrote, it was filled by a read that raced a write,
or a hundred goroutines all found it empty at once.

## Concept notes

_(fill this in as you learn — one section per day, in your own words.)_

_(continue per day)_

## Reference material

- The classic statement of the tradeoffs: cache-aside vs write-through vs
  write-behind, and why "there are only two hard things in computer science"
  includes cache invalidation.
- `golang.org/x/sync/singleflight` — the standard Go answer to stampedes, worth
  reading before writing this stage's own from scratch.

## Status

See [TASKS.md](TASKS.md) for the day-by-day checklist and [../PROGRESS.md](../PROGRESS.md) for where things stand right now.
