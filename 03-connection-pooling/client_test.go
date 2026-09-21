package pool

import (
	"fmt"
	"net"
	"testing"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
	kvstore "github.com/aksharraikanti/distsys-lab/02-kv-store"
)

// otherPeers is the same small local copy 02-kv-store's own test files
// already carry — unexported, in a _test.go file, so not importable
// across packages; duplicating three lines is simpler and more honest
// than exporting a test-only helper from a production package.
func otherPeers(ids []int, self int) []int {
	peers := make([]int, 0, len(ids)-1)
	for _, id := range ids {
		if id != self {
			peers = append(peers, id)
		}
	}
	return peers
}

// waitFor polls condition until it's true or timeout elapses — same
// reasoning as every earlier stage's own waitFor: a fixed sleep either
// wastes time or is flaky.
func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if !condition() {
		t.Fatalf("condition not met within %s", timeout)
	}
}

// tcpKVCluster wires up an n-node Raft/KVServer cluster — using
// FakeTransport for inter-node Raft traffic, exactly like every
// earlier stage's tests, since Day 1's scope is specifically the
// CLIENT-to-KVServer boundary, not node-to-node replication — and
// exposes each node's KVServer over a REAL TCP listener via
// ServeKVServer. Returns each node's real "host:port" address (for
// NewNaiveClient) and a cleanup func.
func tcpKVCluster(t *testing.T, n int) (addrs []string, cleanup func()) {
	t.Helper()
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
			t.Fatalf("ServeKVServer: %v", err)
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
	return addrs, cleanup
}

// TestNaiveClientRoundTrip is Day 1's own sanity check, the real-TCP
// counterpart to 02-kv-store's TestClerkPutAppendGetRoundTrip: a
// NaiveClient dialing real sockets, not calling KVServer methods
// in-process, must still get Put/Append/Get right end to end.
func TestNaiveClientRoundTrip(t *testing.T) {
	addrs, cleanup := tcpKVCluster(t, 3)
	defer cleanup()

	c := NewNaiveClient(addrs)
	c.Put("x", "1")
	c.Append("x", "-more")
	if got := c.Get("x"); got != "1-more" {
		t.Fatalf("Get(x) = %q, want \"1-more\"", got)
	}
	if got := c.Get("nope"); got != "" {
		t.Fatalf("Get(missing key) = %q, want \"\"", got)
	}
}

// TestNaiveClientRetriesPastWrongLeader proves the retry loop actually
// does its job over real TCP: with 3 servers and only one of them
// ever the leader, a client starting from an arbitrary lastKnown index
// must still succeed by trying the others.
func TestNaiveClientRetriesPastWrongLeader(t *testing.T) {
	addrs, cleanup := tcpKVCluster(t, 3)
	defer cleanup()

	for i := 0; i < len(addrs); i++ {
		c := NewNaiveClient(addrs)
		c.lastKnown = i
		key := fmt.Sprintf("key-starting-at-%d", i)
		c.Put(key, "value")
		if got := c.Get(key); got != "value" {
			t.Fatalf("starting lastKnown=%d: Get(%s) = %q, want \"value\"", i, key, got)
		}
	}
}
