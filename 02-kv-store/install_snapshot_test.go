package kvstore

import (
	"testing"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
)

// TestKVServerCatchesUpLaggingFollowerViaInstallSnapshot is Day 7's
// real payoff, at the KV-store level: a follower gets disconnected long
// enough that the leader snapshots well past anything that follower
// ever saw, so AppendEntries alone could never catch it up again — only
// Raft's InstallSnapshot (Day 7) can, and this node's own KVServer
// (server.go's applyLoop, SnapshotValid branch) has to correctly adopt
// what that RPC delivers, not just the Raft layer underneath it.
func TestKVServerCatchesUpLaggingFollowerViaInstallSnapshot(t *testing.T) {
	const maxRaftState = 400
	nodes, kvs, transport, cleanup := newTestCluster(3, maxRaftState)
	defer cleanup()
	waitForSingleLeader(t, nodes, 20*raft.ElectionTimeoutMax)

	ck := NewClerk(kvs)
	ck.Put("x", "seed")

	leaderID := waitForSingleLeader(t, nodes, 20*raft.ElectionTimeoutMax)
	laggingID := -1
	for id := range nodes {
		if id != leaderID {
			laggingID = id
			break
		}
	}

	// Disconnect one follower entirely, then drive enough writes through
	// the remaining majority to force several snapshots on the leader —
	// well past anything the disconnected node ever received.
	transport.Unregister(laggingID)
	for i := 0; i < 150; i++ {
		ck.Append("x", "-bytes-to-force-a-snapshot")
	}

	waitFor(t, 20*raft.ElectionTimeoutMax, func() bool {
		return len(nodes[leaderID].ReadSnapshot()) > 0
	})

	// Reconnect — the lagging node's nextIndex, from the leader's point
	// of view, is now at or below what the leader has already
	// compacted away. Only InstallSnapshot can resolve this.
	transport.Register(laggingID, nodes[laggingID])

	want := ck.Get("x")
	waitFor(t, 80*raft.ElectionTimeoutMax, func() bool {
		v, ok := kvs[laggingID].get("x")
		return ok && v == want
	})

	// And the recovered node must still be a fully functional cluster
	// member afterward — able to serve as leader if elected, not stuck
	// in some special post-catch-up state.
	ck.Put("y", "after-catchup")
	waitFor(t, 20*raft.ElectionTimeoutMax, func() bool {
		v, ok := kvs[laggingID].get("y")
		return ok && v == "after-catchup"
	})
}
