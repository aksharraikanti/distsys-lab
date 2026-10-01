# Stage 6: Hot Shard Detection and Splitting

Package: `hotshard` (import path `github.com/aksharraikanti/distsys-lab/06-hot-shard-splitting`)

## Why this stage

Stage 5 made sharding work: a fixed number of shards, each assigned to a
group, rebalanced by moving whole shards around when groups join or leave.
That's enough as long as the keyspace is loaded roughly evenly — but real
workloads aren't. A single popular key, or a run of keys that happen to hash
near each other, can pin one group as the whole system's bottleneck no
matter how many other groups exist, because Stage 5 can move a shard but can
never make one SMALLER. This stage is about noticing when that's happening
and actually doing something about it: splitting an overloaded shard into
two, each independently assignable, so the groups that were idle can finally
take some of the load.

## Concept notes

_(fill this in as you learn — one section per day, in your own words.)_

_(continue per day)_

## Reference material

- Dynamo-style consistent hashing (Karger et al., "Consistent Hashing and
  Random Trees") — the ring model this stage's shard ranges are built on.
- Bigtable / HBase / CockroachDB's own range-splitting — real systems that
  solve exactly this problem, for the same reason: a fixed partitioning
  scheme can't track a workload that changes after the system was designed.

## Status

See [TASKS.md](TASKS.md) for the day-by-day checklist and [../PROGRESS.md](../PROGRESS.md) for where things stand right now.
