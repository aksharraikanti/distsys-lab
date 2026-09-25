# Stage 4 Tasks: Caching Layer

Builds directly on 03-connection-pooling (and, through it, 02-kv-store and
01-raft). Each day: read/learn the concept first, implement it, write a quick
test, commit. Check a box only once it's implemented AND tested — "read about
it" isn't done.

Package: `cache` (import path `github.com/aksharraikanti/distsys-lab/04-caching`).

The cache sits in FRONT of a KV client (`PooledClient` from Stage 3, behind an
interface so tests can also use a fake store with controllable latency and
failure). Scope for the whole stage: ONE cache instance in a single process —
keeping several cache instances coherent with each other is a different
problem (Stage 9's edge invalidation), not this one.

- [x] **Day 1 — Cache-aside read path.** A `Store` interface
      (`Get`/`Put`/`Append`) that `PooledClient` already satisfies, and a
      `Cache` wrapping any `Store`: on `Get`, look in a local map; on a miss,
      read from the backing store and remember the answer. Unbounded, no
      expiry, writes just pass through — deliberately the simplest thing that
      is still a cache. Hit/miss counters, and a benchmark of a hit vs a miss
      against a real pooled cluster: a concrete number for what a cache hit
      saves, following Stage 3 Day 2's "measure, don't assume" habit.
      Measured (3-node loopback, Apple M2 Pro): uncached pooled `Get`
      ~36-43µs, cache miss ~36µs (no measurable overhead), cache hit
      ~16-22ns — about 2000x. Sharing one client across a cache's callers
      forced a Stage 3 change: `client` is now safe for concurrent use
      (reads concurrent, writes serialized — dedup requires it).
- [x] **Day 2 — Bounded capacity and LRU eviction.** An unbounded cache is a
      memory leak with good intentions. Cap the entry count and evict the
      least-recently-used entry when full (map + doubly linked list, O(1) for
      both lookup and recency update). Prove eviction order precisely, and
      that a `Get` counts as a "use." A hit costs the same ~17-22ns as
      before — the recency bookkeeping is free at this scale. Also fixed a
      Stage 2 test left over from Stage 3 Day 6's `Get` gate.
- [x] **Day 3 — TTL expiry.** Entries go stale even when nobody writes through
      this cache (another client can change the key). Give each entry a
      deadline and treat an expired entry as a miss. Inject the clock as a
      dependency so expiry is tested deterministically instead of with real
      sleeps — this project's flaky-timing history (Stage 1's election
      margins, Stage 2 Day 5's goroutine leak) is exactly why. `New` took
      a third argument here, so it became `New(store, Options{...})` —
      Stage 3 Day 5's `NewPool` lesson applied before the footgun, not
      after. TTL checking costs ~32ns per hit (a clock read): ~16ns -> ~48ns.
- [ ] **Day 4 — Write policies.** What should a `Put`/`Append` do to the cache?
      Implement and compare two: write-invalidate (drop the cached entry, let
      the next read refill it) and write-through (update the cache with the
      new value). `Append` is the interesting case — the cache can't know the
      resulting value without reading it — so decide and document what each
      policy does there. Test read-your-writes through the cache for each,
      and measure the cost difference on a write-heavy vs read-heavy mix.
- [ ] **Day 5 — Invalidation races and stampedes.** Two concurrency bugs every
      cache has to face. (1) Stampede: many goroutines miss on the same hot
      key at once and all hit the backing store; coalesce them into one
      fetch (singleflight). (2) Stale fill: a reader misses and fetches the
      OLD value, a writer then updates the store and invalidates, and the
      reader finally fills the cache with the old value — permanently stale
      until TTL. Guard fills with a per-key version/generation so a fill
      that raced a write is discarded. Reproduce both with a fake `Store`
      whose latency the test controls, so the interleaving is forced rather
      than hoped for.
- [ ] **Day 6 — Load test, hit rate, and faults.** A skewed (Zipf-style)
      workload against the full stack — cache over pooled client over the
      real-TCP Raft cluster — reporting hit rate and latency against the
      uncached stack, then the same workload through Stage 3's fault
      injection (endpoint crashes, leader cut-off, partitions). Invariant: the
      cache never returns a value that violates read-your-writes for its own
      writers, and a backing-store outage degrades to misses/errors rather
      than wedging the cache. This is the stage's — and the whole track's
      first — demoable milestone: a connection-pooled, cached, Raft-backed KV
      store.

## Done means

- `go test ./04-caching/... -race` passes, including the Day 6 load-test suite.
- Stage 4 README's concept notes are filled in for every day.
- `PROGRESS.md` updated, Stage 5 scoped next.
