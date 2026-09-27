package shardkv

import (
	"fmt"
	"sync"
	"testing"
	"time"

	pool "github.com/aksharraikanti/distsys-lab/03-connection-pooling"
	"github.com/aksharraikanti/distsys-lab/internal/kvtest"
)

// testGroups starts n independent 3-node Raft clusters as shard groups 1..n
// and returns them with a static config over them.
func testGroups(t testing.TB, n int) (map[int]*kvtest.Cluster, Config, func()) {
	t.Helper()
	clusters := make(map[int]*kvtest.Cluster, n)
	addrs := make(map[int][]string, n)
	for gid := 1; gid <= n; gid++ {
		c := kvtest.NewCluster(t, 3, -1)
		clusters[gid] = c
		addrs[gid] = c.Addrs
	}
	cleanup := func() {
		for _, c := range clusters {
			c.Close()
		}
	}
	return clusters, NewStaticConfig(addrs), cleanup
}

func TestShardedRoundTrip(t *testing.T) {
	_, cfg, cleanup := testGroups(t, 3)
	defer cleanup()
	c, err := NewShardedClient(cfg)
	if err != nil {
		t.Fatalf("NewShardedClient: %v", err)
	}
	defer c.Close()

	const keys = 60 // enough to land on every shard
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("key-%d", i)
		c.Put(k, fmt.Sprintf("v%d", i))
		c.Append(k, "!")
	}
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("key-%d", i)
		if got, want := c.Get(k), fmt.Sprintf("v%d!", i); got != want {
			t.Fatalf("Get(%s) = %q, want %q", k, got, want)
		}
	}
	if got := c.Get("never-written"); got != "" {
		t.Fatalf("Get(missing) = %q, want \"\"", got)
	}
}

// The core routing property: a key's data lives in exactly the group that owns
// its shard, and in no other. Checked from the side the ShardedClient can't
// cheat on — by asking each group's cluster directly.
func TestKeysLiveOnlyInTheirOwningGroup(t *testing.T) {
	clusters, cfg, cleanup := testGroups(t, 3)
	defer cleanup()
	c, err := NewShardedClient(cfg)
	if err != nil {
		t.Fatalf("NewShardedClient: %v", err)
	}
	defer c.Close()

	raw := map[int]*pool.PooledClient{}
	for gid, cl := range clusters {
		pc, err := pool.NewPooledClient(cl.Addrs, 1, 2)
		if err != nil {
			t.Fatalf("raw client for group %d: %v", gid, err)
		}
		defer pc.Close()
		raw[gid] = pc
	}

	const keys = 60
	perGroup := map[int]int{}
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("key-%d", i)
		c.Put(k, "value")
		perGroup[cfg.Shards[Key2Shard(k)]]++
	}
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("key-%d", i)
		owner := cfg.Shards[Key2Shard(k)]
		for gid, pc := range raw {
			got := pc.Get(k)
			switch {
			case gid == owner && got != "value":
				t.Fatalf("key %s: owning group %d holds %q, want value", k, gid, got)
			case gid != owner && got != "":
				t.Fatalf("key %s leaked into group %d (owner is %d): holds %q", k, gid, owner, got)
			}
		}
	}
	t.Logf("keys per group: %v", perGroup)
	for gid := 1; gid <= 3; gid++ {
		if perGroup[gid] == 0 {
			t.Fatalf("group %d received no keys out of %d: the keyspace is not being spread", gid, keys)
		}
	}
}

// Independent groups mean independent failure: losing one group must affect
// only the shards it owns.
func TestOneGroupDownOnlyAffectsItsOwnShards(t *testing.T) {
	clusters, cfg, cleanup := testGroups(t, 3)
	defer cleanup()
	c, err := NewShardedClient(cfg)
	if err != nil {
		t.Fatalf("NewShardedClient: %v", err)
	}
	defer c.Close()

	const dead = 2
	var deadKey, liveKeys = "", []string{}
	for i := 0; deadKey == "" || len(liveKeys) < 10; i++ {
		if i > 5000 {
			// Bounded, so a routing bug that sends every key to one group
			// fails this test in milliseconds instead of hanging it.
			t.Fatalf("no key routed to group %d (and 10 to others) in 5000 tries: keys are not spreading across groups", dead)
		}
		k := fmt.Sprintf("key-%d", i)
		if cfg.Shards[Key2Shard(k)] == dead {
			if deadKey == "" {
				deadKey = k
			}
		} else if len(liveKeys) < 10 {
			liveKeys = append(liveKeys, k)
		}
	}
	for _, k := range append([]string{deadKey}, liveKeys...) {
		c.Put(k, "before")
	}

	clusters[dead].CrashAll()

	// A read of the dead group's key can only wait...
	blocked := make(chan string, 1)
	go func() { blocked <- c.Get(deadKey) }()

	// ...while every other group carries on, reads AND writes, undisturbed.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for _, k := range liveKeys {
			if got := c.Get(k); got != "before" {
				t.Errorf("live key %s = %q, want before", k, got)
			}
			c.Put(k, "during")
			if got := c.Get(k); got != "during" {
				t.Errorf("live key %s after write = %q, want during", k, got)
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("operations on healthy groups stalled while one group was down")
	}
	select {
	case got := <-blocked:
		t.Fatalf("Get on the dead group's key returned %q; that group is down", got)
	default:
	}

	// The group comes back and the waiting read completes with its old data.
	if err := clusters[dead].RestartAll(); err != nil {
		t.Fatalf("restart: %v", err)
	}
	select {
	case got := <-blocked:
		if got != "before" {
			t.Fatalf("Get after the group recovered = %q, want before (its data must survive the outage)", got)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the read never completed after the dead group came back")
	}
}

// Sharding's promise, measured: writes to different groups proceed in
// parallel, so total write throughput grows with the number of groups.
// (Latency of one write does NOT drop — every write still pays its own
// group's Raft replication. Throughput scales; latency doesn't.)
func TestWriteThroughputScalesWithGroups(t *testing.T) {
	const workers, perWorker = 12, 25

	measure := func(groups int) float64 {
		_, cfg, cleanup := testGroups(t, groups)
		defer cleanup()
		c, err := NewShardedClient(cfg)
		if err != nil {
			t.Fatalf("NewShardedClient: %v", err)
		}
		defer c.Close()

		var wg sync.WaitGroup
		start := time.Now()
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < perWorker; i++ {
					c.Put(fmt.Sprintf("w%d-k%d", w, i), "v")
				}
			}(w)
		}
		wg.Wait()
		return float64(workers*perWorker) / time.Since(start).Seconds()
	}

	one, three := measure(1), measure(3)
	t.Logf("write throughput: 1 group %.0f ops/s, 3 groups %.0f ops/s (%.2fx)", one, three, three/one)
	// Ideal is 3x. Assert well below that: the point is that adding groups
	// clearly helps, not to pin a number that depends on the machine.
	if three < 1.5*one {
		t.Fatalf("3 groups gave %.0f ops/s vs %.0f for 1 (%.2fx); sharding should scale write throughput", three, one, three/one)
	}
}
