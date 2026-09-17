package kvstore

import (
	"testing"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
)

// TestDuplicateRequestNotDoubleApplied proves Day 3's headline
// requirement directly at the Raft layer: the SAME logical request
// (same ClientID+SeqNum) committing at TWO different log indices — Raft
// has no idea it's a retry, it just sees two Propose calls — must only
// mutate the store once.
func TestDuplicateRequestNotDoubleApplied(t *testing.T) {
	rf := raft.NewRaft(0, nil, raft.NewFakeTransport())
	if err := rf.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := rf.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	go rf.RunApplyLoop()
	defer rf.StopElectionTimer()

	kv := NewKVServer(rf, -1)
	defer kv.Stop()

	op := Op{Type: "Append", Key: "x", Value: "a", ClientID: 1, SeqNum: 1}
	if _, _, ok := rf.Propose(op); !ok {
		t.Fatal("first Propose should succeed")
	}
	if _, _, ok := rf.Propose(op); !ok { // simulates a retried RPC landing at a new index
		t.Fatal("duplicate Propose should still succeed at the Raft layer — Raft doesn't know about ClientID/SeqNum")
	}

	waitFor(t, time.Second, func() bool {
		v, ok := kv.get("x")
		return ok && v == "a"
	})

	// Give the second (duplicate) entry time to apply too, and confirm
	// it did NOT double the effect.
	time.Sleep(50 * time.Millisecond)
	if v, _ := kv.get("x"); v != "a" {
		t.Fatalf("value after a duplicate commits = %q, want unchanged %q (applied exactly once)", v, "a")
	}
}

// TestDifferentSeqNumsBothApply proves dedup doesn't over-suppress:
// two DIFFERENT SeqNums from the same client are two genuinely different
// requests and must both take effect.
func TestDifferentSeqNumsBothApply(t *testing.T) {
	rf := raft.NewRaft(0, nil, raft.NewFakeTransport())
	if err := rf.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := rf.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	go rf.RunApplyLoop()
	defer rf.StopElectionTimer()

	kv := NewKVServer(rf, -1)
	defer kv.Stop()

	if _, _, ok := rf.Propose(Op{Type: "Append", Key: "x", Value: "a", ClientID: 1, SeqNum: 1}); !ok {
		t.Fatal("Propose (SeqNum 1) should succeed")
	}
	if _, _, ok := rf.Propose(Op{Type: "Append", Key: "x", Value: "b", ClientID: 1, SeqNum: 2}); !ok {
		t.Fatal("Propose (SeqNum 2) should succeed")
	}

	waitFor(t, time.Second, func() bool {
		v, ok := kv.get("x")
		return ok && v == "ab"
	})
}

// TestDifferentClientsSameSeqNumBothApply proves dedup is scoped PER
// CLIENT, not global: two different clients each using SeqNum 1 (their
// own first request) are unrelated, and both must apply.
func TestDifferentClientsSameSeqNumBothApply(t *testing.T) {
	rf := raft.NewRaft(0, nil, raft.NewFakeTransport())
	if err := rf.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := rf.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	go rf.RunApplyLoop()
	defer rf.StopElectionTimer()

	kv := NewKVServer(rf, -1)
	defer kv.Stop()

	if _, _, ok := rf.Propose(Op{Type: "Append", Key: "x", Value: "a", ClientID: 1, SeqNum: 1}); !ok {
		t.Fatal("Propose (client 1) should succeed")
	}
	if _, _, ok := rf.Propose(Op{Type: "Append", Key: "x", Value: "b", ClientID: 2, SeqNum: 1}); !ok {
		t.Fatal("Propose (client 2) should succeed")
	}

	waitFor(t, time.Second, func() bool {
		v, ok := kv.get("x")
		return ok && v == "ab"
	})
}

// TestStaleRetryAfterNewerRequestSuppressed is the edge case that proves
// the comparison must be "SeqNum > highest seen," not "SeqNum != highest
// seen": a very late duplicate of an OLDER request arriving after a
// NEWER request from the same client has already applied must still be
// suppressed, not mistaken for a new request just because its SeqNum
// differs from the current high-water mark.
func TestStaleRetryAfterNewerRequestSuppressed(t *testing.T) {
	rf := raft.NewRaft(0, nil, raft.NewFakeTransport())
	if err := rf.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := rf.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	go rf.RunApplyLoop()
	defer rf.StopElectionTimer()

	kv := NewKVServer(rf, -1)
	defer kv.Stop()

	if _, _, ok := rf.Propose(Op{Type: "Append", Key: "x", Value: "a", ClientID: 1, SeqNum: 1}); !ok {
		t.Fatal("Propose (SeqNum 1) should succeed")
	}
	if _, _, ok := rf.Propose(Op{Type: "Append", Key: "x", Value: "b", ClientID: 1, SeqNum: 2}); !ok {
		t.Fatal("Propose (SeqNum 2) should succeed")
	}
	// A very late duplicate of SeqNum 1 arrives after SeqNum 2 already
	// committed and applied — this must stay suppressed.
	if _, _, ok := rf.Propose(Op{Type: "Append", Key: "x", Value: "a", ClientID: 1, SeqNum: 1}); !ok {
		t.Fatal("Propose (stale SeqNum 1 retry) should still succeed at the Raft layer")
	}

	waitFor(t, time.Second, func() bool {
		v, ok := kv.get("x")
		return ok && v == "ab"
	})
	time.Sleep(50 * time.Millisecond)
	if v, _ := kv.get("x"); v != "ab" {
		t.Fatalf("value after a stale SeqNum-1 retry post-SeqNum-2 = %q, want unchanged %q", v, "ab")
	}
}

// TestDuplicatePutAppendStillGetsOKReply proves the RPC-facing half of
// Day 3: a client that retries the exact same logical request (same
// ClientID+SeqNum, a second RPC call because it never saw the first
// reply) still gets OK back — the operation DID succeed, just via the
// first attempt — not silently dropped, not an error.
func TestDuplicatePutAppendStillGetsOKReply(t *testing.T) {
	rf := raft.NewRaft(0, nil, raft.NewFakeTransport())
	if err := rf.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := rf.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	go rf.RunApplyLoop()
	defer rf.StopElectionTimer()

	kv := NewKVServer(rf, -1)
	defer kv.Stop()

	args := &PutAppendArgs{Key: "x", Value: "a", Op: "Append", ClientID: 1, SeqNum: 1}

	var reply1 PutAppendReply
	if err := kv.PutAppend(args, &reply1); err != nil {
		t.Fatalf("PutAppend (first): %v", err)
	}
	if reply1.Err != OK {
		t.Fatalf("first PutAppend Err = %q, want OK", reply1.Err)
	}

	var reply2 PutAppendReply
	if err := kv.PutAppend(args, &reply2); err != nil {
		t.Fatalf("PutAppend (retry): %v", err)
	}
	if reply2.Err != OK {
		t.Fatalf("retried PutAppend Err = %q, want OK (the operation already succeeded via the first attempt)", reply2.Err)
	}

	v, ok := kv.get("x")
	if !ok || v != "a" {
		t.Fatalf("value after 2 identical RPC calls = (%q,%v), want (\"a\", true) — applied exactly once despite 2 calls", v, ok)
	}
}
