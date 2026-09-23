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

### Day 1 — Cache-aside read path
- The first real design question wasn't about caching at all. A `Cache`
  shares ONE `Store` among all its callers, but Stage 3's client was
  documented as not safe for concurrent use. The obvious failure is a data
  race on `lastKnown`; the nasty one is `seqNum`. Stage 2's dedup keeps only
  the *highest* SeqNum applied per ClientID, so if one client had write 4 and
  write 5 in flight and 5 committed first, write 4 would be discarded as an
  already-seen "duplicate" — an acknowledged write silently lost, with no
  race detector in sight. So the fix isn't just a mutex: writes must be
  *serialized* per client (the protocol assumes one at a time per ClientID),
  while reads, which carry no SeqNum, can run concurrently. I made the client
  safe first rather than papering over it in the cache — the cache is the
  first component to share a client, but it shouldn't be the only thing that
  knows the client is fragile.
- The cache never holds its lock while calling the store. Holding it is
  simpler and would work, but one slow miss would then block every other
  caller — including ones asking for keys already in memory that should be
  instant. That would make the cache a global bottleneck exactly when the
  backing store is slow, which is when a cache matters most.
- Caching the empty string for a missing key is deliberate: without it every
  read of an absent key is a permanent miss and pays a full round trip. It is
  also a first taste of the stage's central problem — if the key is created
  later, the cached "" is now wrong.
- `TestWriteLeavesCachedValueStale` asserts the *wrong* behavior on purpose.
  Day 1 ships writes that bypass the cache, so read-your-writes is broken
  through this very `Cache`. Rather than leave that as a comment someone might
  miss, the test pins it down; Day 4 flips it to assert the right thing.
- The numbers: a hit is ~16-22ns against ~36-43µs for an uncached pooled read
  (~2000x), and a miss costs the same as no cache at all — the bookkeeping is
  lost in the noise of the round trip. So on read-heavy traffic the cache is
  close to free downside; the price is entirely correctness, which is the rest
  of the stage.

_(continue per day)_

## Reference material

- The classic statement of the tradeoffs: cache-aside vs write-through vs
  write-behind, and why "there are only two hard things in computer science"
  includes cache invalidation.
- `golang.org/x/sync/singleflight` — the standard Go answer to stampedes, worth
  reading before writing this stage's own from scratch.

## Status

See [TASKS.md](TASKS.md) for the day-by-day checklist and [../PROGRESS.md](../PROGRESS.md) for where things stand right now.
