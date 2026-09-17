package kvstore

import (
	"testing"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
)

// singleNodeLeaderKV builds a one-node Raft "cluster" already promoted
// to Leader, wires a KVServer on top of it with maxRaftState, and
// starts both background loops (RunApplyLoop on the raft side,
// applyLoop/noopLoop via NewKVServer). Returns both so tests can
// Propose directly (bypassing PutAppend/Clerk, which Day 6 doesn't
// need) and inspect kv's state.
func singleNodeLeaderKV(t *testing.T, persister raft.Persister, maxRaftState int) (*raft.Raft, *KVServer) {
	t.Helper()
	var rf *raft.Raft
	if persister != nil {
		rf = raft.NewRaftWithPersister(0, nil, raft.NewFakeTransport(), persister)
	} else {
		rf = raft.NewRaft(0, nil, raft.NewFakeTransport())
	}
	if err := rf.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := rf.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	go rf.RunApplyLoop()
	kv := NewKVServer(rf, maxRaftState)
	return rf, kv
}

// TestKVServerSnapshotsWhenRaftStateExceedsThreshold proves the size
// policy actually fires: proposing far more entries than maxRaftState
// allows must keep rf's RaftStateSize bounded, not let it grow without
// limit — if applyLoop's threshold check (or snapshotLocked itself)
// were broken, this is what would catch it, rather than the compaction
// silently never happening.
func TestKVServerSnapshotsWhenRaftStateExceedsThreshold(t *testing.T) {
	const maxRaftState = 400
	rf, kv := singleNodeLeaderKV(t, nil, maxRaftState)
	defer rf.StopElectionTimer()
	defer kv.Stop()

	for i := 0; i < 200; i++ {
		op := Op{Type: "Append", Key: "x", Value: "some bytes to grow the log", ClientID: 1, SeqNum: int64(i + 1)}
		if _, _, ok := rf.Propose(op); !ok {
			t.Fatalf("Propose(%d): not leader", i)
		}
	}

	// Give applyLoop time to catch up and snapshot repeatedly — 200
	// entries at maxRaftState=400 should trigger several compactions,
	// not just one right at the very end.
	waitFor(t, time.Second, func() bool {
		return rf.RaftStateSize() < maxRaftState*3
	})

	if got := rf.RaftStateSize(); got >= maxRaftState*3 {
		t.Fatalf("RaftStateSize = %d after 200 proposals with maxRaftState=%d, want well under (snapshotting should be keeping this bounded)", got, maxRaftState)
	}
}

// TestKVServerNeverSnapshotsWhenDisabled proves maxRaftState=-1 (what
// every pre-Day-6 test in this package still passes) really does
// disable snapshotting — RaftStateSize should grow roughly linearly
// with the number of proposals, unbounded, since nothing is compacting
// it. This is the control for the test above: without it, a bug that
// made maxRaftState=-1 accidentally still trigger snapshots wouldn't
// be caught by anything else in this package.
func TestKVServerNeverSnapshotsWhenDisabled(t *testing.T) {
	rf, kv := singleNodeLeaderKV(t, nil, -1)
	defer rf.StopElectionTimer()
	defer kv.Stop()

	for i := 0; i < 50; i++ {
		op := Op{Type: "Append", Key: "x", Value: "some bytes to grow the log", ClientID: 1, SeqNum: int64(i + 1)}
		if _, _, ok := rf.Propose(op); !ok {
			t.Fatalf("Propose(%d): not leader", i)
		}
	}
	waitFor(t, time.Second, func() bool {
		v, ok := kv.get("x")
		return ok && len(v) == 50*len("some bytes to grow the log")
	})

	// 50 entries of ~25 bytes each is comfortably past what a
	// snapshotting policy at any reasonable threshold would have
	// compacted away — with snapshotting disabled, RaftStateSize must
	// still reflect the full, uncompacted log.
	if got := rf.RaftStateSize(); got < 50*25 {
		t.Fatalf("RaftStateSize = %d after 50 proposals with snapshotting disabled, want it to reflect the full uncompacted log (>= ~1250)", got)
	}
}

// TestKVServerRestoresFromSnapshotAfterRestart is Day 6's real payoff:
// a node snapshots, "crashes" (discarded, reconstructed against the
// same persister — same mechanism as 01-raft's own restart tests), and
// the NEW KVServer must recover the exact same store, even for keys
// whose only surviving Put/Append entries were compacted out of the
// log and can never arrive on ApplyCh again.
func TestKVServerRestoresFromSnapshotAfterRestart(t *testing.T) {
	const maxRaftState = 300
	persister := raft.NewMemoryPersister()
	rf, kv := singleNodeLeaderKV(t, persister, maxRaftState)

	for i := 0; i < 100; i++ {
		op := Op{Type: "Append", Key: "x", Value: "bytes-to-force-a-snapshot", ClientID: 1, SeqNum: int64(i + 1)}
		if _, _, ok := rf.Propose(op); !ok {
			t.Fatalf("Propose(%d): not leader", i)
		}
	}
	var want string
	waitFor(t, time.Second, func() bool {
		v, ok := kv.get("x")
		want = v
		return ok && len(v) == 100*len("bytes-to-force-a-snapshot")
	})
	// Confirm a snapshot actually happened before "crashing" — otherwise
	// this test would trivially pass via ordinary log replay, proving
	// nothing about restore-from-snapshot at all.
	waitFor(t, time.Second, func() bool {
		return len(rf.ReadSnapshot()) > 0
	})

	// The "crash": stop and discard both rf and kv, reconstruct fresh
	// against the same persister.
	rf.StopElectionTimer()
	kv.Stop()

	restartedRf := raft.NewRaftWithPersister(0, nil, raft.NewFakeTransport(), persister)
	go restartedRf.RunApplyLoop()
	defer restartedRf.StopElectionTimer()
	restartedKV := NewKVServer(restartedRf, maxRaftState)
	defer restartedKV.Stop()

	// No new entries have been proposed to restartedRf, and any
	// pre-crash entries still in its log haven't even committed yet
	// (single-node, but commitIndex/lastApplied only advance once this
	// node is Leader again) — so if the value is right, it can only have
	// come from the restored snapshot, not a replay.
	got, ok := restartedKV.get("x")
	if !ok || got != want {
		t.Fatalf("restarted KVServer get(x) = (%q, %v), want (%q, true) restored from snapshot alone", got, ok, want)
	}
}

// TestKVServerDedupTableSurvivesSnapshotAndRestart proves the OTHER
// half of kvSnapshot round-trips too: duplicateTable, not just store.
// Without it, a client whose retry happens to land after a restart
// that followed a snapshot could have its already-applied write
// silently re-applied — exactly the double-Append bug Day 3 exists to
// prevent, reopened by Day 6 if the dedup table weren't part of the
// snapshot.
func TestKVServerDedupTableSurvivesSnapshotAndRestart(t *testing.T) {
	const maxRaftState = 300
	persister := raft.NewMemoryPersister()
	rf, kv := singleNodeLeaderKV(t, persister, maxRaftState)

	for i := 0; i < 100; i++ {
		op := Op{Type: "Append", Key: "x", Value: "bytes-to-force-a-snapshot", ClientID: 42, SeqNum: int64(i + 1)}
		if _, _, ok := rf.Propose(op); !ok {
			t.Fatalf("Propose(%d): not leader", i)
		}
	}
	var want string
	waitFor(t, time.Second, func() bool {
		v, ok := kv.get("x")
		want = v
		return ok && len(v) == 100*len("bytes-to-force-a-snapshot")
	})
	waitFor(t, time.Second, func() bool {
		return len(rf.ReadSnapshot()) > 0
	})

	rf.StopElectionTimer()
	kv.Stop()

	restartedRf := raft.NewRaftWithPersister(0, nil, raft.NewFakeTransport(), persister)
	go restartedRf.RunApplyLoop()
	defer restartedRf.StopElectionTimer()
	restartedKV := NewKVServer(restartedRf, maxRaftState)
	defer restartedKV.Stop()

	if err := restartedRf.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := restartedRf.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}

	// Replay client 42's LAST pre-crash SeqNum as a "retry" — the dedup
	// table must recognize it as already-applied and skip it, or "x"
	// would gain a duplicate copy of that fragment.
	retry := Op{Type: "Append", Key: "x", Value: "bytes-to-force-a-snapshot", ClientID: 42, SeqNum: 100}
	if _, _, ok := restartedRf.Propose(retry); !ok {
		t.Fatal("Propose(retry): not leader")
	}
	// And a genuinely NEW request from the same client must still apply.
	fresh := Op{Type: "Append", Key: "x", Value: "-fresh", ClientID: 42, SeqNum: 101}
	if _, _, ok := restartedRf.Propose(fresh); !ok {
		t.Fatal("Propose(fresh): not leader")
	}

	wantAfter := want + "-fresh"
	waitFor(t, time.Second, func() bool {
		v, ok := restartedKV.get("x")
		return ok && v == wantAfter
	})
	// waitFor above only confirms the eventual value is consistent with
	// the retry having been skipped; give the (already-satisfied)
	// condition one more explicit check for a clear failure message if
	// dedup silently let the retry double-apply instead.
	if got, ok := restartedKV.get("x"); !ok || got != wantAfter {
		t.Fatalf("get(x) after restart+retry+fresh = (%q, %v), want (%q, true) — a mismatch here means the retry double-applied, i.e. duplicateTable did NOT survive the snapshot", got, ok, wantAfter)
	}
}
