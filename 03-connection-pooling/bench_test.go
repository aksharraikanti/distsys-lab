package pool

import (
	"net"
	"testing"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
	kvstore "github.com/aksharraikanti/distsys-lab/02-kv-store"
)

// benchTCPKVCluster is tcpKVCluster's benchmark-flavored twin: same
// FakeTransport-for-Raft, real-TCP-for-KVServer shape, but also waits
// for a leader to be elected before returning, so a benchmark's timed
// loop (started via b.ResetTimer after this returns) measures steady-
// state call latency, not a one-time election delay baked into
// whichever b.N happens to hit the untimed setup window.
func benchTCPKVCluster(b *testing.B, n int) (addrs []string, cleanup func()) {
	b.Helper()
	transport := raft.NewFakeTransport()
	ids := make([]int, n)
	for i := range ids {
		ids[i] = i
	}
	nodes := make(map[int]*raft.Raft, n)
	kvs := make([]*kvstore.KVServer, n)
	listeners := make([]net.Listener, n)
	addrs = make([]string, n)
	for _, id := range ids {
		rf := raft.NewRaft(id, otherPeers(ids, id), transport)
		nodes[id] = rf
		transport.Register(id, rf)
		kvs[id] = kvstore.NewKVServer(rf, -1)
		l, err := ServeKVServer("127.0.0.1:0", kvs[id])
		if err != nil {
			b.Fatalf("ServeKVServer: %v", err)
		}
		listeners[id] = l
		addrs[id] = l.Addr().String()
	}
	for _, rf := range nodes {
		go rf.RunElectionTimer()
		go rf.RunHeartbeats()
		go rf.RunApplyLoop()
	}
	cleanup = func() {
		for _, rf := range nodes {
			rf.StopElectionTimer()
		}
		for _, kv := range kvs {
			kv.Stop()
		}
		for _, l := range listeners {
			l.Close()
		}
	}

	leaderElected := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, rf := range nodes {
			if rf.State() == raft.Leader {
				leaderElected = true
			}
		}
		if leaderElected {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !leaderElected {
		cleanup()
		b.Fatal("no leader elected before benchmark start")
	}
	return addrs, cleanup
}

// BenchmarkNaiveClientPutAppend measures the cold-start cost this
// stage exists to improve on: every single Append here pays a fresh
// TCP dial (SYN/SYN-ACK/ACK, even over loopback) before the RPC itself
// ever runs — though see BenchmarkNaiveClientGet/BenchmarkPooledClientGet
// for why a WRITE benchmark alone actually understates how much that
// matters (a write's own latency floor is dominated by Raft's
// replication round trip, not the client's connection cost).
func BenchmarkNaiveClientPutAppend(b *testing.B) {
	addrs, cleanup := benchTCPKVCluster(b, 3)
	defer cleanup()

	c := NewNaiveClient(addrs)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Append("bench-key", "x")
	}
}

// BenchmarkNaiveClientGet is BenchmarkNaiveClientPutAppend's read-path
// twin. Get never goes through Propose/replication (Get is a direct
// local read once a node believes itself leader — see 02-kv-store's
// own Get doc comment) — so, unlike a write, a read's latency is
// almost ENTIRELY the client's own connection cost, with no Raft
// consensus round trip underneath it to dwarf that cost. This is the
// benchmark that actually isolates what pooling can improve.
func BenchmarkNaiveClientGet(b *testing.B) {
	addrs, cleanup := benchTCPKVCluster(b, 3)
	defer cleanup()

	c := NewNaiveClient(addrs)
	c.Put("bench-key", "x") // give Get something real to read
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get("bench-key")
	}
}
