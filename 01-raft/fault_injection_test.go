package raft

import (
	"reflect"
	"testing"
	"time"
)

// waitForSingleLeader polls until exactly one node in nodes is Leader and
// returns its id. Shared by every fault-injection test below — each one
// needs to detect the current leader repeatedly, since leadership
// changes hands across the crashes/partitions/restarts they inject.
func waitForSingleLeader(t *testing.T, nodes map[int]*Raft, timeout time.Duration) int {
	t.Helper()
	leaderID := -1
	waitFor(t, timeout, func() bool {
		leaders := 0
		for id, n := range nodes {
			if n.State() == Leader {
				leaders++
				leaderID = id
			}
		}
		return leaders == 1
	})
	return leaderID
}

// waitForAllCommitAtLeast polls until every node in nodes has a
// commitIndex >= index.
func waitForAllCommitAtLeast(t *testing.T, nodes map[int]*Raft, index int, timeout time.Duration) {
	t.Helper()
	waitFor(t, timeout, func() bool {
		for _, n := range nodes {
			if n.CommitIndex() < index {
				return false
			}
		}
		return true
	})
}

// assertLogsConsistent is the Log Matching Property this entire stage
// has been building toward, checked directly: every node's log must
// agree, entry for entry, through the lowest commitIndex any of them has
// reached. Anything beyond that point is allowed to differ transiently
// (an uncommitted entry can still legitimately be overwritten later) —
// only the COMMITTED prefix has to be identical everywhere.
func assertLogsConsistent(t *testing.T, nodes map[int]*Raft) {
	t.Helper()

	minCommit := -1
	for _, n := range nodes {
		c := n.CommitIndex()
		if minCommit == -1 || c < minCommit {
			minCommit = c
		}
	}
	if minCommit <= 0 {
		return // nothing committed everywhere yet — nothing to compare
	}

	var reference []LogEntry
	referenceID := -1
	for id, n := range nodes {
		n.mu.Lock()
		log := append([]LogEntry(nil), n.log[:minCommit]...)
		n.mu.Unlock()
		if reference == nil {
			reference = log
			referenceID = id
			continue
		}
		if !reflect.DeepEqual(log, reference) {
			t.Fatalf("log mismatch through commit index %d: node %d has %+v, node %d has %+v",
				minCommit, referenceID, reference, id, log)
		}
	}
}

// TestFaultInjectionLeaderCrashElectsNewLeader simulates the first fault
// TASKS.md names: the leader crashes. The rest of the cluster must elect
// a new leader and keep committing new client commands — this is the
// entire reason Raft tolerates a minority of failures at all.
func TestFaultInjectionLeaderCrashElectsNewLeader(t *testing.T) {
	transport := NewFakeTransport()
	ids := []int{0, 1, 2}
	nodes := make(map[int]*Raft, len(ids))
	for _, id := range ids {
		n := NewRaft(id, otherPeers(ids, id), transport)
		nodes[id] = n
		transport.Register(id, n)
	}
	for _, n := range nodes {
		go n.RunElectionTimer()
		go n.RunHeartbeats()
		go n.RunApplyLoop()
	}
	defer func() {
		for _, n := range nodes {
			n.StopElectionTimer()
		}
	}()

	firstLeaderID := waitForSingleLeader(t, nodes, 20*ElectionTimeoutMax)

	if _, _, ok := nodes[firstLeaderID].Propose("before-crash"); !ok {
		t.Fatal("Propose should succeed on the first leader")
	}
	waitForAllCommitAtLeast(t, nodes, 1, 20*ElectionTimeoutMax)

	// Crash the leader: unreachable at the transport level AND stop its
	// own background loops — matching what a real process exit looks
	// like, not just a node that stopped initiating calls but would
	// still (incorrectly) answer them.
	nodes[firstLeaderID].StopElectionTimer()
	transport.Unregister(firstLeaderID)
	delete(nodes, firstLeaderID)

	newLeaderID := waitForSingleLeader(t, nodes, 20*ElectionTimeoutMax)
	if newLeaderID == firstLeaderID {
		t.Fatalf("expected a NEW leader after the crash, still observing the crashed node %d", firstLeaderID)
	}

	if _, _, ok := nodes[newLeaderID].Propose("after-crash"); !ok {
		t.Fatal("Propose should succeed on the new leader")
	}
	waitForAllCommitAtLeast(t, nodes, 2, 20*ElectionTimeoutMax)

	assertLogsConsistent(t, nodes)
}

// TestFaultInjectionNetworkPartitionMajorityContinues simulates the
// second fault TASKS.md names: a network partition. A 5-node cluster
// splits so the current leader lands in a 2-node MINORITY. The 3-node
// MAJORITY must elect its own leader and keep committing; the isolated
// old leader must NOT be able to commit anything new (it can't reach a
// majority alone). Once healed, the old leader steps down and the whole
// cluster converges on one consistent log.
func TestFaultInjectionNetworkPartitionMajorityContinues(t *testing.T) {
	transport := NewFakeTransport()
	ids := []int{0, 1, 2, 3, 4}
	nodes := make(map[int]*Raft, len(ids))
	for _, id := range ids {
		n := NewRaft(id, otherPeers(ids, id), transport)
		nodes[id] = n
		transport.Register(id, n)
	}
	for _, n := range nodes {
		go n.RunElectionTimer()
		go n.RunHeartbeats()
		go n.RunApplyLoop()
	}
	defer func() {
		for _, n := range nodes {
			n.StopElectionTimer()
		}
	}()

	leaderID := waitForSingleLeader(t, nodes, 20*ElectionTimeoutMax)

	if _, _, ok := nodes[leaderID].Propose("before-partition"); !ok {
		t.Fatal("Propose should succeed")
	}
	waitForAllCommitAtLeast(t, nodes, 1, 20*ElectionTimeoutMax)

	var minority, majority []int
	for _, id := range ids {
		if id == leaderID {
			minority = append(minority, id)
		} else {
			majority = append(majority, id)
		}
	}
	minority = append(minority, majority[0]) // old leader + 1 more = 2-node minority
	majority = majority[1:]                  // remaining 3 nodes = majority
	transport.Partition(minority, majority)
	defer transport.Heal()

	majorityNodes := make(map[int]*Raft, len(majority))
	for _, id := range majority {
		majorityNodes[id] = nodes[id]
	}
	newLeaderID := waitForSingleLeader(t, majorityNodes, 20*ElectionTimeoutMax)
	if newLeaderID == leaderID {
		t.Fatalf("expected the MAJORITY side to elect a fresh leader, got the isolated old leader %d", leaderID)
	}

	if _, _, ok := nodes[newLeaderID].Propose("during-partition"); !ok {
		t.Fatal("Propose should succeed on the majority-side leader")
	}
	waitForAllCommitAtLeast(t, majorityNodes, 2, 20*ElectionTimeoutMax)

	// The old leader doesn't know it's partitioned — Propose still
	// succeeds locally (it appends to its own log). What must NOT happen
	// is this entry ever committing: it can't reach a majority alone.
	if _, _, ok := nodes[leaderID].Propose("stuck-in-minority"); !ok {
		t.Fatal("Propose still succeeds locally on the isolated old leader (expected — it has no way to know it's cut off)")
	}
	time.Sleep(10 * ElectionTimeoutMax) // several replication rounds' worth
	if got := nodes[leaderID].CommitIndex(); got != 1 {
		t.Fatalf("isolated old leader's commitIndex = %d, want unchanged at 1 — it must not be able to commit without a majority", got)
	}

	transport.Heal()

	// The old leader hears from the new, higher-term leader on the next
	// replication attempt that actually reaches someone, and steps down.
	waitFor(t, 20*ElectionTimeoutMax, func() bool {
		return nodes[leaderID].State() == Follower
	})
	waitForAllCommitAtLeast(t, nodes, 2, 20*ElectionTimeoutMax)

	assertLogsConsistent(t, nodes)
}

// TestFaultInjectionFollowerRestartMidOperation simulates the third
// fault TASKS.md names: a follower restarts. Combined with Day 11's
// persistence, this is the fullest version of that scenario — a real
// crash-and-restart, not just a fresh in-memory node — while the leader
// keeps taking client commands the whole time the follower is down. The
// restarted follower must catch up via replication (persistence alone
// only recovers what it had BEFORE crashing, not what happened while it
// was gone).
func TestFaultInjectionFollowerRestartMidOperation(t *testing.T) {
	transport := NewFakeTransport()
	ids := []int{0, 1, 2}
	persisters := make(map[int]*MemoryPersister, len(ids))
	nodes := make(map[int]*Raft, len(ids))
	for _, id := range ids {
		p := NewMemoryPersister()
		persisters[id] = p
		n := NewRaftWithPersister(id, otherPeers(ids, id), transport, p)
		nodes[id] = n
		transport.Register(id, n)
	}
	for _, n := range nodes {
		go n.RunElectionTimer()
		go n.RunHeartbeats()
		go n.RunApplyLoop()
	}
	defer func() {
		for _, n := range nodes {
			n.StopElectionTimer()
		}
	}()

	leaderID := waitForSingleLeader(t, nodes, 20*ElectionTimeoutMax)

	if _, _, ok := nodes[leaderID].Propose("cmd-1"); !ok {
		t.Fatal("Propose should succeed")
	}
	waitForAllCommitAtLeast(t, nodes, 1, 20*ElectionTimeoutMax)

	var restartID int
	for _, id := range ids {
		if id != leaderID {
			restartID = id
			break
		}
	}

	nodes[restartID].StopElectionTimer()
	transport.Unregister(restartID)

	// The leader keeps taking client commands while the follower is
	// down — with 3 nodes, the leader + the one remaining follower is
	// still a majority, so this must keep working.
	if _, _, ok := nodes[leaderID].Propose("cmd-2"); !ok {
		t.Fatal("Propose should succeed while the follower is down")
	}
	if _, _, ok := nodes[leaderID].Propose("cmd-3"); !ok {
		t.Fatal("Propose should succeed while the follower is down")
	}

	// Restart: a fresh instance, same id, SAME persister — recovers
	// term/votedFor/log as they were AT CRASH TIME, but still has to
	// catch up on cmd-2/cmd-3 via replication, not persistence.
	restarted := NewRaftWithPersister(restartID, otherPeers(ids, restartID), transport, persisters[restartID])
	nodes[restartID] = restarted
	transport.Register(restartID, restarted)
	go restarted.RunElectionTimer()
	go restarted.RunHeartbeats()
	go restarted.RunApplyLoop()

	waitForAllCommitAtLeast(t, nodes, 3, 20*ElectionTimeoutMax)

	assertLogsConsistent(t, nodes)
}
