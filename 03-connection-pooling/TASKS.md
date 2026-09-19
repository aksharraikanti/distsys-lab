# Stage 3 Tasks: Connection Pooling Layer

Builds directly on 02-kv-store (and, through it, 01-raft). Each day: read/learn
the concept first, implement it, write a quick test, commit. Check a box only
once it's implemented AND tested — "read about it" isn't done.

Package: `pool` (import path `github.com/aksharraikanti/distsys-lab/03-connection-pooling`).

- [ ] **Day 1 — Real network boundary.** Every earlier stage's `Clerk` has
      called `KVServer` methods directly, in-process — no actual socket
      between client and server. Pooling a connection means nothing without a
      real connection to pool, so this day comes first: expose `KVServer`
      over real TCP via `net/rpc`, mirroring 01-raft's own
      `NetTransport`/`ServeNetTransport` pattern (`ServeKVServer(addr, kv)`
      on the server side). A bare client dials fresh per request — no pooling
      yet — establishing the cold-start baseline every later day measures
      against.
- [ ] **Day 2 — Naive fixed-size pool.** A pool of N already-dialed
      connections, checked out before a request and checked back in after,
      reused across requests instead of dialing per call. Benchmark
      pooled-vs-cold-start latency for the same request volume — a concrete
      before/after number, not just "pooling should be faster."
- [ ] **Day 3 — Backpressure under exhaustion.** More concurrent requests than
      the pool has connections is the normal case under load, not an edge
      case — decide and implement what happens: block-and-wait with a bound
      (timeout or context cancellation), an overflow queue, or reject-and-let-
      the-caller-retry. Prove the choice holds under real concurrent load,
      not just that it compiles.
- [ ] **Day 4 — Health checking and eviction.** A pooled connection can go bad
      out from under the pool — the KV node behind it crashed or restarted
      (Stage 2's own fault injection is the natural source of this). Detect a
      broken connection (a failed call, not just a missing heartbeat) and
      evict + replace it rather than handing a known-bad connection to the
      next caller.
- [ ] **Day 5 — Idle eviction and pool sizing.** Reuse-vs-cold-start cuts both
      ways: an idle connection held open forever wastes resources on both
      ends. Close connections that sit idle past a threshold; make min/max
      pool size configurable rather than a single hardcoded N, and prove the
      pool actually shrinks and regrows across a load/idle/load cycle.
- [ ] **Day 6 — Load test through real faults.** Combine Stage 2's own
      concurrent-client and fault-injection patterns (Day 5/Day 8's tests)
      with the pool sitting in between: concurrent callers hammering the pool
      while a backing KV node crashes, restarts, or gets partitioned away.
      The invariant: no connection leak, no deadlock under backpressure, and
      the pool recovers (evicts the bad connection, serves the next caller
      correctly) once the node comes back — this stage's own version of Day
      8's full-integration test.

## Done means

- `go test ./03-connection-pooling/... -race` passes, including the Day 6
  load-test-through-faults suite.
- Stage 3 README's concept notes are filled in for every day.
- `PROGRESS.md` updated, Stage 4 scoped next.
