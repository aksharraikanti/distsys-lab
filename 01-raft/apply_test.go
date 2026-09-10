package raft

import (
	"testing"
	"time"
)

// TestApplyLoopDeliversInOrder proves RunApplyLoop delivers committed
// entries on ApplyCh in order, exactly once, and advances lastApplied to
// match — using a manually pre-populated log/commitIndex to isolate the
// apply loop from the rest of the commit pipeline (that pipeline is
// covered end-to-end by TestEndToEndProposeCommitApply below).
func TestApplyLoopDeliversInOrder(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	r.mu.Lock()
	r.log = []LogEntry{{Term: 1, Command: "a"}, {Term: 1, Command: "b"}, {Term: 1, Command: "c"}}
	r.commitIndex = 3
	r.mu.Unlock()

	go r.RunApplyLoop()
	defer r.StopElectionTimer()

	for i, want := range []string{"a", "b", "c"} {
		select {
		case msg := <-r.ApplyCh:
			if msg.Index != i+1 || msg.Term != 1 || msg.Command != want {
				t.Fatalf("applied message %d = %+v, want Index=%d Term=1 Command=%q", i, msg, i+1, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for applied entry %d", i)
		}
	}

	waitFor(t, time.Second, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.lastApplied == 3
	})
}

// TestApplyLoopStopsCleanly proves RunApplyLoop exits promptly once
// StopElectionTimer closes stopCh — no goroutine leak, whether or not
// there was anything left to apply.
func TestApplyLoopStopsCleanly(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	done := make(chan struct{})
	go func() {
		r.RunApplyLoop()
		close(done)
	}()

	r.StopElectionTimer()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunApplyLoop did not exit promptly after StopElectionTimer")
	}
}

// TestEndToEndProposeCommitApply is Day 9's headline proof, and closes
// the loop this whole stage has been building toward: a client Proposes
// a command on the leader, it replicates, a majority confirms it, the
// leader commits it, a follower's next AppendEntries carries that commit
// forward via LeaderCommit, and EVERY node — leader and both followers —
// applies the exact same entry, in order, on its own ApplyCh.
func TestEndToEndProposeCommitApply(t *testing.T) {
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

	leaderID := -1
	waitFor(t, 20*ElectionTimeoutMax, func() bool {
		leaders := 0
		for id, n := range nodes {
			if n.State() == Leader {
				leaders++
				leaderID = id
			}
		}
		return leaders == 1
	})

	wantIndex, wantTerm, ok := nodes[leaderID].Propose("set x=42")
	if !ok {
		t.Fatal("Propose should succeed on the leader")
	}

	for _, id := range ids {
		var msg ApplyMsg
		select {
		case msg = <-nodes[id].ApplyCh:
		case <-time.After(20 * ElectionTimeoutMax):
			t.Fatalf("node %d never applied the committed entry", id)
		}
		if msg.Index != wantIndex || msg.Term != wantTerm || msg.Command != "set x=42" {
			t.Fatalf("node %d applied %+v, want {Index:%d Term:%d Command:\"set x=42\"}", id, msg, wantIndex, wantTerm)
		}
	}
}
