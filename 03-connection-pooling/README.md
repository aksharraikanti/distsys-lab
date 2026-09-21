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
