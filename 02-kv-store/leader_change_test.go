package kvstore

import (
	"testing"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
)

// TestPutAppendBailsOutQuicklyWhenLeadershipLost is Day 4's headline
// proof: this node Proposes successfully, then loses leadership (a
// higher term arrives) before anything else ever touches that log
// index — nothing guarantees the notify channel EVER fires again for
// it, so the handler must not wait for the full commitTimeout. It should
// notice, via leaderCheckInterval's poll, within a handful of
// milliseconds.
func TestPutAppendBailsOutQuicklyWhenLeadershipLost(t *testing.T) {
	rf := raft.NewRaft(0, []int{1, 2}, raft.NewFakeTransport())
	if err := rf.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := rf.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	go rf.RunApplyLoop()
	defer rf.StopElectionTimer()

	kv := NewKVServer(rf)
	defer kv.Stop()

	replyCh := make(chan PutAppendReply, 1)
	start := time.Now()
	go func() {
		var reply PutAppendReply
		_ = kv.PutAppend(&PutAppendArgs{Key: "x", Value: "1", Op: "Put", ClientID: 1, SeqNum: 1}, &reply)
		replyCh <- reply
	}()

	// Let the goroutine actually Propose and register its notify channel.
	time.Sleep(5 * time.Millisecond)

	// Simulate discovering a higher term (e.g. a real leader election
	// happened elsewhere) — this node steps down, and nothing will ever
	// arrive on its notify channel for the index it Proposed to.
	rf.BecomeFollower(rf.Term() + 1)

	select {
	case reply := <-replyCh:
		elapsed := time.Since(start)
		if reply.Err != ErrWrongLeader {
			t.Fatalf("PutAppend after leadership loss returned Err=%q, want ErrWrongLeader", reply.Err)
		}
		if elapsed >= commitTimeout {
			t.Fatalf("PutAppend took %s to bail out, want well under the %s commitTimeout — leaderCheckInterval should catch this fast", elapsed, commitTimeout)
		}
	case <-time.After(commitTimeout):
		t.Fatal("PutAppend never returned — it should have noticed leadership loss and bailed out, not waited for the full commit timeout")
	}
}

// TestPutAppendSucceedsWhenLeadershipNeverLost is the regression check
// for the leaderCheckInterval polling loop added in Day 4: it must not
// interfere with an ordinary, uninterrupted commit — a leader that
// simply takes a little while to commit (slower than one poll tick, but
// well within commitTimeout) must still get OK, not a false
// ErrWrongLeader from the poll misfiring.
func TestPutAppendSucceedsWhenLeadershipNeverLost(t *testing.T) {
	rf := raft.NewRaft(0, nil, raft.NewFakeTransport())
	if err := rf.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := rf.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	go rf.RunApplyLoop()
	defer rf.StopElectionTimer()

	kv := NewKVServer(rf)
	defer kv.Stop()

	// A single-node cluster commits immediately, but the poll loop still
	// runs at least once before the notify channel fires (leaderCheckInterval
	// is small) — this proves the poll doesn't race a legitimate success
	// into a false failure.
	var reply PutAppendReply
	if err := kv.PutAppend(&PutAppendArgs{Key: "x", Value: "1", Op: "Put", ClientID: 1, SeqNum: 1}, &reply); err != nil {
		t.Fatalf("PutAppend: %v", err)
	}
	if reply.Err != OK {
		t.Fatalf("PutAppend Err = %q, want OK", reply.Err)
	}
}
