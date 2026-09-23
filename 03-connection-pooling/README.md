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

### Day 1 — Real network boundary
- `KVServer.Get`/`PutAppend` already had exactly the shape `net/rpc`
  requires (exported method, on an exported type, `*Args` in, `*Reply`
  out, `error` returned) — so `ServeKVServer` needed no wrapper type at
  all, unlike 01-raft's `raftRPCService`. That wrapper existed there
  because `NetTransport`/`FakeTransport` both had to satisfy the SAME
  `raft.RPCHandler` interface, and `net/rpc.RegisterName` needs a
  concrete receiver to reflect over. `KVServer` never had an interface
  boundary to begin with (`Clerk` always called it directly), so there
  was nothing to bridge — registering `kv` itself was enough.
- `NaiveClient` turned out to be almost a line-for-line copy of
  `Clerk`, and that similarity is the actual finding, not incidental:
  the RETRY logic (round-robin from `lastKnown`, retry on
  `ErrWrongLeader`, ClientID+SeqNum for dedup) has nothing to do with
  whether the transport is in-process or a real socket — it's entirely
  about not knowing which node currently leads. The ONE thing that
  changed is what counts as "this attempt failed, try the next server":
  `Clerk` only had `reply.Err` to look at, but `NaiveClient` can ALSO
  fail at the dial or the call itself (`rpc.Dial`/`client.Call`
  returning an error) — a failure mode that plainly can't exist without
  a real socket. Treating that the same way `Clerk` already treats an
  unreachable-in-FakeTransport peer (skip to the next server, don't
  error out) was the only real design decision this day needed to make.
- Measuring the cold-start baseline (`BenchmarkNaiveClientPutAppend`,
  ~3.1ms/op on loopback) before writing a single line of pooling code
  was worth doing explicitly rather than skipping to "obviously pooling
  will be faster." Even over loopback, on the same machine, dialing
  fresh every call is measurably expensive — the TCP handshake cost is
  real, not theoretical, which is exactly the number the rest of this
  stage needs to actually move.

### Day 2 — Naive fixed-size pool
- Building `NaiveClient` and `PooledClient` back to back is what made
  factoring out the shared retry/dedup logic (round-robin from
  `lastKnown`, retry on `ErrWrongLeader`, ClientID+SeqNum) an easy call
  rather than a judgment call: Day 1's own README note had already
  observed it was transport-agnostic, and writing a SECOND client that
  would otherwise duplicate it verbatim is exactly the point where
  "three similar lines" turns into "a whole retry loop copy-pasted a
  second time." The one axis that actually varies — how a single RPC
  gets made — is now a one-method `caller` interface
  (`dialPerCallCaller` vs `pooledCaller`), and both `NaiveClient` and
  `PooledClient` are thin wrappers over the same shared `client`.
- The benchmark comparison split cleanly by operation, and the split
  itself is the real finding, not just the numbers: `PutAppend` showed
  almost no improvement from pooling (~3.1ms either way), while `Get`
  improved roughly 2-3x (~150-220μs down to ~50-80μs). The reason once
  I looked: `PutAppend` goes through `Propose` and has to wait for the
  entry to replicate to a majority before it can return — a real
  network round trip to the other Raft nodes that's at least one
  `HeartbeatInterval`, dwarfing whatever a client's own connection
  setup costs. `Get` (per 02-kv-store's own doc comment) is a direct
  local read once a node believes itself leader — no Raft round trip at
  all — so the CLIENT's connection cost is almost the entire story,
  and reusing a connection has real, visible room to matter. A
  benchmark that only measured writes would have made pooling look
  nearly pointless; measuring both is what actually shows what it's
  for.
- `PooledClient` inherits the exact same "not safe for concurrent use"
  contract `Clerk` (02-kv-store) already documents, for the identical
  reason (`seqNum`/`lastKnown` aren't synchronized) — and I re-learned
  this the hard way, not just by remembering it: an early version of
  `TestPooledClientHandlesMoreConcurrentCallersThanPoolSize` shared ONE
  `PooledClient` across several goroutines and `-race` caught the
  corruption immediately. Split the test into two levels instead: raw
  concurrent `Pool.Call` pressure (proving the pool itself handles
  contention safely) and a SEPARATE check with one `PooledClient` per
  goroutine (proving the higher-level retry logic still works under
  the same load) — conflating the two would have tested something
  murkier than either property on its own.
- Stress-running the whole suite repeatedly while validating this day
  surfaced a real, if narrow, gap in Day 8's own
  `TestFullIntegrationSurvivesWholeClusterRestart` (02-kv-store): it
  waited for `CommitIndex` to catch up after a simulated restart, but
  commitIndex catching up doesn't mean `KVServer.store` has — that
  still has to flow through Raft's own `applyPending` (its own
  `HeartbeatInterval`-paced lag behind commitIndex, by design) and then
  `KVServer.applyLoop` consuming `ApplyCh`. The fix was to poll the
  actual observable outcome (`Get` returning the expected value) rather
  than an intermediate implementation detail one hop removed from what
  actually mattered — the same "wait for the real thing, not a proxy
  for it" lesson this whole project keeps relearning at different
  layers (Day 8's own README notes already named the shallower version
  of this same gap; this was the version underneath it).

### Day 3 — Backpressure under exhaustion
- Naming the three real options before picking one made the choice
  easy rather than arbitrary. An overflow queue sounds like it "fixes"
  exhaustion, but it doesn't — it just relocates the problem from
  blocked goroutines (bounded, visible, and something the caller can
  time out on) to unbounded queue growth (no natural backpressure at
  all, and a very different failure mode under sustained overload).
  Immediate rejection is honest about failing fast, but it can't tell
  "busy for 2ms because of a normal burst" from "actually broken," and
  punishes the former as harshly as the latter. Block-with-timeout is
  the only one of the three that actually bounds BOTH the wait and the
  resource usage at once — that's not a compromise between the other
  two, it's a strictly better shape for this specific problem.
- Threading `checkoutTimeout` through `NewPool`/`NewPooledClient`
  turned what was an implicit behavior (Day 2's `<-p.free` blocking
  forever, correct by accident rather than by design) into something
  the type signature itself now forces every caller to decide on. That
  friction is the point, not a downside — Day 2's own doc comment had
  already flagged the unbounded block as "not yet a deliberate policy,"
  and a required constructor argument is what actually makes it one.
- `defaultCheckoutTimeout` is derived from `raft.HeartbeatInterval` (10x
  it) rather than an unrelated new magic number, for the same reason
  `client.go`'s own retry sleep already uses that constant: it needs to
  be long enough that a real, short burst (on the order of a heartbeat
  round) has a genuine chance to drain, but short enough that an
  exhausted pool doesn't dominate a request's total latency before the
  caller's OWN retry loop gets a chance to fall back to a different
  server. The two timeouts (pool checkout, client retry cycle) aren't
  independent numbers — picking one without the other in mind would
  have been guessing.
- Testing this needed two different timeout scales in the same file:
  a SHORT one (30ms) to prove `ErrPoolExhausted` actually fires within
  a bounded, predictable window without making the test itself slow,
  and a GENEROUS one (1s) for the tests that are really about
  correctness under contention, not about timing — where a short
  timeout would just make the test flaky for a reason that has nothing
  to do with what it's supposed to be proving.

### Day 4 — Health checking and eviction
- Distinguishing "this connection is broken" from "this call got a
  perfectly normal not-the-leader answer" turned out to need zero
  actual logic, once I looked at what `net/rpc.Client.Call` and
  `02-kv-store`'s own RPC contract actually guarantee together:
  `KVServer.Get`/`PutAppend` NEVER return a non-nil Go `error` — every
  outcome, including "wrong leader," travels through `reply.Err`
  instead. So any non-nil `error` `Call` itself returns can only be a
  transport-level failure. No error-type inspection, no string
  matching on `rpc.ErrShutdown` — just "err != nil means evict." That
  simplicity isn't an accident of this stage; it's a direct payoff of a
  design decision Stage 2 made back on Day 2 for an unrelated reason
  (distinguishing "not committed yet" from "not leader" cleanly), now
  reused for something Stage 2 never anticipated.
- Testing "the pool recovers once a crashed node comes back" needed a
  genuinely realistic way to simulate a broken connection, and the
  first idea (`l.Close()` on the server's listener) turned out to be
  wrong: closing a `net.Listener` only stops ACCEPTING new connections
  — `net/rpc`'s already-accepted connections keep being served by their
  own goroutines, completely unaffected. An already-established pooled
  connection would have kept working fine even after "crashing" the
  server this way. The fix was closing the connection directly,
  client-side — which is actually MORE realistic, not a shortcut: in
  a real crash, the client never gets an authoritative "the server is
  gone" signal either, it just observes its own read/write failing,
  which is exactly what closing the client's own `*rpc.Client` produces.
- The background `redialUntilSuccess` goroutine is the one piece of
  real concurrency machinery this stage has needed so far, and it
  earns its complexity by closing a correctness gap that a simpler
  "retry once, give up" design would have left open: Stage 2's fault
  injection holds a crashed node down for real time (not an instant),
  so a caller that only retries the redial once, synchronously, while
  the node is down would leave the pool permanently one connection
  short — repeat that for every connection in the pool and it ends up
  at zero, forever, even after the node recovers. That's not a
  theoretical concern; Day 6's own "the pool recovers... once the node
  comes back" requirement depends directly on this NOT happening. Close
  needed its own `stopCh`/`sync.WaitGroup` specifically to shut this
  goroutine down cleanly — the same shape `raft.Raft.StopElectionTimer`
  and `KVServer.Stop` already established, applied to a new kind of
  background work.

### Day 5 — Idle eviction and pool sizing
- `NewPool`'s signature had been quietly accumulating risk across Days
  3-5: by the time this day started it took `(addr, size,
  checkoutTimeout, redialInterval)`, and I was about to add a FIFTH
  parameter (`idleTimeout`) — three `time.Duration`s in a row, which is
  exactly the shape that makes a call site typo-swappable and silent
  (the compiler can't catch `NewPool(addr, size, redialInterval,
  checkoutTimeout)` — same types, wrong order). Replacing the parameter
  list with a `PoolOptions` struct wasn't a stylistic preference, it
  was fixing a real, now-actually-present risk, not a hypothetical
  future one — the exact distinction that decides whether an
  abstraction is premature or overdue.
- Day 5's elasticity forced a real reconsideration of Day 4's own
  guarantee, not just an addition on top of it. Day 4 said "a broken
  connection is ALWAYS replaced" — correct and necessary for a
  fixed-size pool, where every connection is load-bearing by
  definition. But once the pool can legitimately hold MORE than it
  strictly needs (grown under a since-passed burst), always replacing
  a broken one above `MinSize` would mean the pool never actually
  shrinks via breakage — only via idle timeout — which is an arbitrary,
  unintended asymmetry between two things that should behave the same
  way: "this connection isn't needed anymore." The fix was letting
  `evictAndReplace` check `count < MinSize` before deciding to redial
  at all, so a broken connection above the minimum just shrinks the
  pool by one, same as an idle eviction would.
- Growth and shrinkage turned out not to need coordinating with each
  other explicitly, which I initially expected would be the hard part.
  `count`, protected by one mutex, is the single source of truth both
  `checkout`'s growth path and `evictIdleConnections`'s shrink path
  read and write — as long as every path that changes how many
  connections exist goes through the same counter under the same lock,
  growth and shrinkage are just two independent forces pushing on the
  same number, not two subsystems that need to know about each other.
- Proving the load/idle/load cycle needed a real end-to-end test
  (`TestPoolLoadIdleLoadCycle`, real concurrent `Call`s, not just raw
  `checkout`/`checkin`) in addition to the two more surgical ones
  (`TestPoolGrowsOnDemandUpToMaxSize`, `TestPoolShrinksIdleConnectionsDownToMinSize`).
  The surgical tests prove the mechanism works in isolation; the cycle
  test is what actually matches Day 5's own stated goal — and it's
  also the one that would have caught a regression where the pool
  shrinks once and then, wrongly, never grows back (an easy bug to
  introduce if the growth path ever assumed "we're already at some
  size" instead of re-checking the CURRENT count every time).

### Day 6 — Load test through real faults
- The first version of the test passed in 0.12 seconds, and that speed was
  the tell: the first fault was scheduled after a 250ms tick, and the whole
  workload finished before it fired. It was a load test of a cluster with
  no faults, reporting success. Fixed by firing the first fault immediately
  AND asserting that at least three fault rounds actually ran — a test
  should fail when it stops testing what it claims to. I then checked it
  could fail at all by mutating `Pool.Call` to skip eviction: the test
  deadlocked (dead connections handed out forever), so it really does
  exercise Day 4's machinery.
- Simulating a *crashed* endpoint needed test-side machinery because
  `ServeKVServer` can't do it: closing the listener leaves already-accepted
  connections alive (Day 4's lesson, now from the server side). The test's
  `crashableEndpoint` tracks every accepted connection so `crash()` severs
  them all and refuses new dials, and `start()` re-listens on the same
  address — what a client of a really-restarted process sees.
- **The test found a real Stage 2 bug.** About one run in eight, several
  clients simultaneously read back a value missing their last acknowledged
  Append, always by exactly one fragment. Before touching anything I
  re-read after each mismatch to see whether it converged: every one did,
  so nothing was lost — a read was answered by a freshly-elected leader
  that hadn't applied the last entry its predecessor committed. Stage 2
  Day 5's `noopLoop` was supposed to close that window, but it only
  *proposed* the no-op; `Get` never waited for it. The actual Raft §8 rule
  is that a leader must apply an entry from its own term before serving
  reads. Fixed with a `noopAppliedTerm` gate in `Get` (which then revealed
  the `"Noop"` case in `applyLoop`'s switch had always been dead code — a
  no-op has SeqNum 0, so the dedup guard skips it before the switch). The
  failure rate went from ~1/8 to 0/50. Endpoint crashes are what exposed
  it: they force clients off the leader mid-flight, which Stage 2's
  in-process `Clerk` never did.
- One more flaw fell out of stress-running everything together: Day 5's
  `TestPoolLoadIdleLoadCycle` read the pool size once after the load
  finished, but under CPU contention the 30ms idle evictor could shrink the
  pool back before that read. It now records the peak size *during* the
  load. Same lesson as Stage 2 Day 8 in a new place — assert on what
  happened, not on a snapshot taken after the system has moved on.

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

### Later addendum — the client became safe for concurrent use
Day 2 documented `PooledClient` as not safe for concurrent use, mirroring
`Clerk`. Stage 4 Day 1 reversed that: a cache in front of a client is one
shared client with many callers. Reads run concurrently; writes are
serialized per client because dedup keeps only the highest SeqNum per
ClientID — concurrent writes could commit out of order and lose the lower
one. See Stage 4's README for the reasoning.
