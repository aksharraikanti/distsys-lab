package hotshard

import (
	"testing"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
	shardkv "github.com/aksharraikanti/distsys-lab/05-sharded-kv"
)

func newRingCtrlerCluster(n int) (ctrlers []*RingCtrler, nodes []*raft.Raft, cleanup func()) {
	transport := raft.NewFakeTransport()
	ids := make([]int, n)
	for i := range ids {
		ids[i] = i
	}
	for _, id := range ids {
		var peers []int
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		rf := raft.NewRaft(id, peers, transport)
		transport.Register(id, rf)
		nodes = append(nodes, rf)
		ctrlers = append(ctrlers, NewRingCtrler(rf))
	}
	for _, rf := range nodes {
		go rf.RunElectionTimer()
		go rf.RunHeartbeats()
		go rf.RunApplyLoop()
	}
	return ctrlers, nodes, func() {
		for _, rf := range nodes {
			rf.StopElectionTimer()
		}
		for _, c := range ctrlers {
			c.Stop()
		}
	}
}

// do retries op against every node until one isn't WrongLeader/Timeout,
// returning that verdict — the retry loop a clerk would run.
func do(t *testing.T, ctrlers []*RingCtrler, op func(c *RingCtrler) shardkv.CtrlerErr) shardkv.CtrlerErr {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, c := range ctrlers {
			if err := op(c); err != shardkv.CtrlerErrWrongLeader && err != shardkv.CtrlerErrTimeout {
				return err
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("no leader accepted the request")
	return ""
}

func query(t *testing.T, ctrlers []*RingCtrler, num int) RingConfig {
	t.Helper()
	var cfg RingConfig
	if err := do(t, ctrlers, func(c *RingCtrler) shardkv.CtrlerErr {
		var r QueryReply
		c.Query(&QueryArgs{Num: num}, &r)
		cfg = r.Config
		return r.Err
	}); err != shardkv.CtrlerOK {
		t.Fatalf("query: %v", err)
	}
	return cfg
}

func TestRingCtrlerInitSplitMove(t *testing.T) {
	ctrlers, _, cleanup := newRingCtrlerCluster(3)
	defer cleanup()

	ring := NewRing(4)
	if err := do(t, ctrlers, func(c *RingCtrler) shardkv.CtrlerErr {
		var r InitReply
		c.Init(&InitArgs{Ring: ring, Groups: testGroups, ClientID: 1, SeqNum: 1}, &r)
		return r.Err
	}); err != shardkv.CtrlerOK {
		t.Fatalf("init: %v", err)
	}

	mid, _ := Midpoint(ring, 3)
	if err := do(t, ctrlers, func(c *RingCtrler) shardkv.CtrlerErr {
		var r SplitReply
		c.Split(&SplitArgs{Shard: 3, Point: mid, ClientID: 1, SeqNum: 2}, &r)
		return r.Err
	}); err != shardkv.CtrlerOK {
		t.Fatalf("split: %v", err)
	}

	cfg := query(t, ctrlers, -1)
	if cfg.Num != 2 || len(cfg.Ring.Entries) != 5 {
		t.Fatalf("after init+split: Num=%d shards=%d, want 2 and 5", cfg.Num, len(cfg.Ring.Entries))
	}
	if cfg.Owners[5] != cfg.Owners[3] {
		t.Fatalf("new shard owner %d != original %d", cfg.Owners[5], cfg.Owners[3])
	}

	// The new half can then move on its own, as an ordinary Move.
	if err := do(t, ctrlers, func(c *RingCtrler) shardkv.CtrlerErr {
		var r MoveReply
		c.Move(&MoveArgs{Shard: 5, GID: 2, ClientID: 1, SeqNum: 3}, &r)
		return r.Err
	}); err != shardkv.CtrlerOK {
		t.Fatalf("move: %v", err)
	}
	cfg = query(t, ctrlers, -1)
	if cfg.Num != 3 || cfg.Owners[5] != 2 {
		t.Fatalf("after move: %+v", cfg)
	}
	// History is preserved: version 1 is the pre-split ring.
	if old := query(t, ctrlers, 1); len(old.Ring.Entries) != 4 {
		t.Fatalf("version 1 has %d shards, want 4", len(old.Ring.Entries))
	}
}

// A committed-but-invalid Split must report the failure AND leave the
// history unchanged — the apply-time check Stage 5's Move couldn't do.
func TestRingCtrlerStaleSplitRejectedAtApply(t *testing.T) {
	ctrlers, _, cleanup := newRingCtrlerCluster(3)
	defer cleanup()
	ring := NewRing(2)
	do(t, ctrlers, func(c *RingCtrler) shardkv.CtrlerErr {
		var r InitReply
		c.Init(&InitArgs{Ring: ring, Groups: testGroups, ClientID: 1, SeqNum: 1}, &r)
		return r.Err
	})
	mid, _ := Midpoint(ring, 1)
	split := func(seq int64) shardkv.CtrlerErr {
		return do(t, ctrlers, func(c *RingCtrler) shardkv.CtrlerErr {
			var r SplitReply
			c.Split(&SplitArgs{Shard: 1, Point: mid, ClientID: 2, SeqNum: seq}, &r)
			return r.Err
		})
	}
	if err := split(1); err != shardkv.CtrlerOK {
		t.Fatalf("first split: %v", err)
	}
	// Same point again: it's now a shard boundary, not interior to shard 1.
	if err := split(2); err != shardkv.CtrlerErrInvalidArgs {
		t.Fatalf("stale split returned %v, want ErrInvalidArgs", err)
	}
	// A retry of that same failed request must hear the same verdict, not
	// a false OK from the dedup table.
	if err := split(2); err != shardkv.CtrlerErrInvalidArgs {
		t.Fatalf("retry of failed split returned %v, want ErrInvalidArgs", err)
	}
	if cfg := query(t, ctrlers, -1); cfg.Num != 2 {
		t.Fatalf("stale split changed history: Num=%d, want 2", cfg.Num)
	}
	// Re-Init on a live history is also rejected.
	if err := do(t, ctrlers, func(c *RingCtrler) shardkv.CtrlerErr {
		var r InitReply
		c.Init(&InitArgs{Ring: ring, Groups: testGroups, ClientID: 3, SeqNum: 1}, &r)
		return r.Err
	}); err != shardkv.CtrlerErrInvalidArgs {
		t.Fatalf("re-init returned %v", err)
	}
}

// A retried Split (same ClientID+SeqNum) must apply once, and answer the
// retry with the original verdict.
func TestRingCtrlerSplitDedup(t *testing.T) {
	ctrlers, _, cleanup := newRingCtrlerCluster(3)
	defer cleanup()
	ring := NewRing(2)
	do(t, ctrlers, func(c *RingCtrler) shardkv.CtrlerErr {
		var r InitReply
		c.Init(&InitArgs{Ring: ring, Groups: testGroups, ClientID: 1, SeqNum: 1}, &r)
		return r.Err
	})
	mid, _ := Midpoint(ring, 1)
	for i := 0; i < 3; i++ {
		if err := do(t, ctrlers, func(c *RingCtrler) shardkv.CtrlerErr {
			var r SplitReply
			c.Split(&SplitArgs{Shard: 1, Point: mid, ClientID: 2, SeqNum: 1}, &r)
			return r.Err
		}); err != shardkv.CtrlerOK {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	if cfg := query(t, ctrlers, -1); cfg.Num != 2 || len(cfg.Ring.Entries) != 3 {
		t.Fatalf("retries applied more than once: Num=%d shards=%d", cfg.Num, len(cfg.Ring.Entries))
	}
}

// Every replica must converge on identical history.
func TestRingCtrlerReplicasAgree(t *testing.T) {
	ctrlers, _, cleanup := newRingCtrlerCluster(3)
	defer cleanup()
	ring := NewRing(3)
	do(t, ctrlers, func(c *RingCtrler) shardkv.CtrlerErr {
		var r InitReply
		c.Init(&InitArgs{Ring: ring, Groups: testGroups, ClientID: 1, SeqNum: 1}, &r)
		return r.Err
	})
	mid, _ := Midpoint(ring, 2)
	do(t, ctrlers, func(c *RingCtrler) shardkv.CtrlerErr {
		var r SplitReply
		c.Split(&SplitArgs{Shard: 2, Point: mid, ClientID: 1, SeqNum: 2}, &r)
		return r.Err
	})
	waitUntil(t, func() bool {
		for _, c := range ctrlers {
			c.mu.Lock()
			n := len(c.configs)
			c.mu.Unlock()
			if n != 3 {
				return false
			}
		}
		return true
	})
	ref := ctrlers[0].configs[2]
	for i, c := range ctrlers {
		c.mu.Lock()
		got := c.configs[2]
		c.mu.Unlock()
		if got.Num != ref.Num || len(got.Ring.Entries) != len(ref.Ring.Entries) {
			t.Fatalf("replica %d diverged", i)
		}
		for j, e := range got.Ring.Entries {
			if e != ref.Ring.Entries[j] || got.Owners[e.Shard] != ref.Owners[e.Shard] {
				t.Fatalf("replica %d diverged at entry %d", i, j)
			}
		}
	}
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met")
}
