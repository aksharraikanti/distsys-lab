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

### Day 2 — Bounded capacity and LRU eviction
- The classic LRU is a map plus a doubly linked list, and the whole design is
  keeping the two in agreement: the map gives O(1) lookup, the list gives
  O(1) "move to front" and "drop the back", and the list node stores its key
  so that evicting the back node can also delete the right map entry. The bug
  that structure invites is a node in the list that the map no longer points
  at — an orphan — because eviction then deletes by *key* and can remove a
  live entry that happens to share it. So the tests check the invariant
  directly (same size, every node is the one its key maps to) rather than
  only checking behavior.
- The orphan isn't hypothetical here. Day 1 left concurrent misses on the same
  key uncoalesced, so two goroutines can both miss, both fetch, and both
  insert. Without a branch in `insertLocked` that updates the existing node,
  the second insert pushes a duplicate. I proved the test catches that by
  deleting the branch: two tests failed with "map has 1 entries but list has 2
  nodes". Worth doing for any test guarding a subtle bug — a test that has
  never failed hasn't shown it can.
- A hit now takes the exclusive lock. Marking an entry as used mutates the
  recency list, so an LRU can't offer readers a shared `RWMutex` fast path the
  way a plain map could. The lock is still only held for map and list work,
  never across a store call, and a hit still costs ~17-22ns, so this doesn't
  matter at this scale. It would matter under heavy multi-core read
  contention, which is the standard reason real caches shard or sample their
  recency updates. Noted, not built.
- Evicted-then-read-again is a normal miss, and `Evictions` is counted so the
  Day 6 load test can report it: a hit rate is only interpretable next to how
  much the cache is thrashing.
- A hung test taught a small lesson about my own test double: `gatedStore`'s
  `entered` channel had buffer 2, but later calls in the same test sent on it
  three more times with no receiver, so the test deadlocked on a send. Sizing
  a signalling channel for exactly the calls you're thinking about, instead of
  all the calls that will happen, is an easy mistake.
- Stage 2's `TestPutAppendAndGetRoundTrip` flaked once in 20 runs. That was on
  me from Stage 3 Day 6: it calls `Get` right after `PutAppend`, but `Get` now
  refuses until the leader's no-op applies, and the no-op's tick can land after
  the writes. It passed all of Day 6's stress runs by timing luck. Fixed by
  waiting for a real answer, then 300/300 clean. When you change a contract,
  grep every caller — stress runs only find the ones the scheduler happens to
  expose.

### Day 3 — TTL expiry
- Why a TTL at all, when Day 4 will make this cache's own writes update or
  invalidate it? Because a cache can only fix staleness it *causes*. Any
  other client can change a key without ever passing through this process, and
  nothing here will ever hear about it. A TTL is the only tool that bounds how
  long such a value can be served, and it's what turns Day 1's "stale forever"
  gap into "stale for at most one TTL".
- Three semantics were each a real decision, and each has a test that fails
  if you flip it (I checked by mutating all three): the deadline counts from
  fetch *start* (a slow fetch returned something that was only known to be
  current sometime during it — counting from the end overstates freshness);
  a hit does *not* extend the deadline (refresh-on-read would keep the most
  frequently read keys, whose staleness is most visible, cached forever); and
  at exactly the deadline the entry is already expired ("served for less than
  d"). The third is a coin flip that matters only because it's a place two
  implementations silently disagree.
- Expiry is lazy: checked on read, removed on read. No background sweeper —
  that's a goroutine with a lifecycle (Stage 3's `Close`/`WaitGroup`
  machinery) spent to reclaim memory that `Capacity` already bounds. The cost
  is that an expired entry nobody reads again occupies a slot until LRU evicts
  it. It's never *served*, only *held*. Worth revisiting only if the cache
  gets large enough that dead entries crowd out live ones.
- The injected `Clock` is why the tests are instant and exact: advance a fake
  clock by 59s, then 2s, and assert. Real-sleep expiry tests are how this repo
  got flaky before (Stage 1's election margins, Stage 3's idle evictor). One
  test does use the real clock, to prove the nil default is wired up — but
  only in the direction that can't spuriously fail: sleeping never
  undershoots, so "expired after 60ms" is safe to assert, while "still fresh
  within 20ms" would flake under load.
- `Options` instead of a third positional argument: `New(store, capacity)` was
  about to become `New(store, capacity, ttl, clock)`. I did the refactor
  before it was a footgun, because Stage 3 Day 5 showed what waiting costs.
- One of my own mutation checks was bogus and I nearly took its silence as a
  pass: the mutated `Get` left a variable unused, so the package didn't
  compile, no test ran, and "no failures printed" looked like "not caught".
  A mutation result only counts if the mutated code built. I redid it with
  `_ = start` and the right test failed. Silence from a test run is not
  evidence unless you know the test ran.
- Cost: TTL checking is a clock read on every hit — ~32ns (a hit goes from
  ~16ns to ~48ns). A cache with TTL 0 never reads the clock, so it pays
  nothing for a feature it isn't using.

### Day 4 — Write policies
- `Append` decided the design. A cache can hold the result of a `Put` (you
  just wrote it) but not of an `Append` (the result is old+value, and you
  don't know old). The tempting shortcut is `cached + value`, and it's wrong in
  a way no single-client test would notice: if another client appended in the
  meantime, or the entry is merely stale, you cache a value that never existed
  in the store — and then serve it as truth.
  `TestAppendDoesNotBuildOnAStaleCachedValue` builds exactly that scenario. The
  alternative, reading the result back, is correct but pays a round trip per
  Append to prefetch something that may never be read — the very cost
  write-through is meant to avoid. So `Append` invalidates under both
  policies, and the next read pays one honest miss.
- The two policies really are a tradeoff, not a ranking, and counting store
  reads makes that concrete. When written keys are read back, write-through
  wins outright (0 store reads vs 200 for 200 write-then-read pairs). When
  writes go to keys nobody reads, write-through *loses*: it inserts every
  written key, and in a bounded cache those displace the hot read set (0 store
  reads for write-invalidate vs 287 for write-through). Which one is right
  depends on whether your writes predict your reads. Invalidate is the zero
  value because it can never cache something wrong — it can only cost a miss.
- I measured store reads instead of latency on purpose. A write here costs
  ~3ms of Raft replication regardless of policy, so timing would have measured
  Raft and made both policies look identical.
- My first threshold for the pollution test was a guess (`>= 400`, "nearly
  every round") and it failed at 287. I'd reasoned that each write-only key
  evicts a hot one every round, but a miss refills the hot key and evicts a
  write-only one, so the cache partly recovers. The measurement was right and
  my model was wrong; I changed the assertion to what's true ("hundreds, where
  before it was zero") instead of nudging the number until it passed.
- Order is store-first, then cache, and there's a test for it: freeze a `Put`
  inside the store write, read in the gap, then release. Invalidating first
  would let a reader in that gap miss, fetch the old value, and re-cache it,
  with nothing left to remove it after the write lands. Flipping the order in
  `Put` fails that test under both policies — checked by mutation.
- Store-first narrows a race but can't close it, and
  `TestStaleFillRaceIsAKnownGap` is the honest record of that: a reader
  fetches "old", a writer updates store and cache, the reader inserts "old"
  over the top, and the cache disagrees with the store until TTL. Like Day 1's
  stale-write test it asserts the wrong behavior on purpose. Day 5's version
  guard flips it.
- Day 1's `TestWriteLeavesCachedValueStale` is gone, replaced by
  `TestReadYourWrites` (both policies, both write operations) and
  `TestWriteFixesCachedAbsence` — the cached-"" case Day 1 flagged as the
  first taste of this stage's central problem.
- A process slip worth recording: my docs script for this day failed an
  assertion (I'd quoted the wrong line of TASKS.md) and aborted, but the shell
  chain continued with `;` and shipped the code without the notes. Caught it
  from the missing PROGRESS entry and fixed it in a follow-up PR. Chaining
  steps with `;` after a step that can fail turns "it errored" into "it
  shipped anyway"; the fix is `set -e`, or `&&` all the way through.

### Day 5 — Invalidation races and stampedes
- The version-per-key fix from the task description would have worked and
  would also have leaked: a version for every key ever written, forever. The
  in-flight registry gives the same guarantee with state that only exists
  while a fetch is running. A write marks that key's in-flight fetch stale;
  when the fetch finishes it returns its value to its own caller (whose read
  overlapped the write, so the old value is a legal answer) but doesn't cache
  it. The soundness argument is an ordering one: a fetch registers *before*
  it reads the store, and a write marks fetches *after* its store write has
  landed — so any fetch that could have seen pre-write data is already
  registered by the time the write goes looking for it.
- Marking the flight stale wasn't enough, and this is the subtlest thing in
  the stage. The writer's own next `Get` would find that still-running flight
  and *join* it, being handed the pre-write value — read-your-writes broken for
  the exact goroutine that wrote. The write has to detach the flight too, so
  later readers start a fresh fetch. My first mutation check of this printed
  nothing, which I nearly read as "not caught"; it was actually a *hang* (the
  writer's Get blocked forever behind the stale fetch), which `go test` reports
  differently from a failure. I rewrote the test to fail in seconds with a
  message instead. A test that can only fail by timing out is a bad test.
- The detach test only catches the bug under `WriteInvalidate`. Under
  `WriteThrough` the `Put` populates the cache, so the writer's next `Get` is a
  hit and never reaches the flight logic. That's inherent, not a hole, but it's
  the kind of thing a comment should say rather than leave for the next reader
  to rediscover.
- The write-write reordering race wasn't on the task list. I found it by asking
  what *else* could make a cache permanently disagree with its store, and it's
  real: writer 1's store write lands first but its cache update is delayed;
  writer 2 lands v2 and updates the cache to v2; writer 1 then "updates" it to
  v1. The store says v2, the cache says v1, no TTL heals it under
  `WriteThrough`. Serializing writes fixes it, and costs nothing here because
  the KV client already serializes writes per ClientID (Stage 4 Day 1). Readers
  never take the lock.
- Coalescing needed failure handling I almost skipped: if the leader's fetch
  panics and joiners wait on a channel nobody closes, they hang forever. The
  cleanup is deferred and a `flight.ok` flag tells joiners the fetch failed, so
  they retry (one becomes the new leader). It's only reachable if a `Store`
  panics, which the KV client never does — but "can't happen with today's
  store" is precisely how a hang ships.
- The measurement: 50 goroutines missing on one key against the real Raft +
  TCP + pool stack produced **1** cluster read (`hits=0 misses=50
  coalesced=49`). The test asserts "far fewer than half" rather than exactly
  one, because a goroutine arriving after the fetch completes legitimately hits
  the cache.
- A whole-system property test ties it together: after a burst of concurrent
  reads, writes, and appends on a few keys, every entry still in the cache must
  equal the store's value. Checked at quiescence, where any disagreement is
  permanent because there's no TTL to heal it.
- Verification, for the record: all five guards (stale-cached-anyway, no
  detach, unserialized writes, coalescing off, joiners-trust-failed-leader)
  were broken one at a time and each is caught. Two of my first attempts were
  invalid — one didn't compile because of a shell-escaping slip, one hung —
  and were redone. Same lesson as Day 3: a mutation result only counts if the
  mutant built and the test ran to a verdict.

_(continue per day)_

## Reference material

- The classic statement of the tradeoffs: cache-aside vs write-through vs
  write-behind, and why "there are only two hard things in computer science"
  includes cache invalidation.
- `golang.org/x/sync/singleflight` — the standard Go answer to stampedes, worth
  reading before writing this stage's own from scratch.

## Status

See [TASKS.md](TASKS.md) for the day-by-day checklist and [../PROGRESS.md](../PROGRESS.md) for where things stand right now.
