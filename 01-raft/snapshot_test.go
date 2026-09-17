package raft

import (
	"reflect"
	"testing"
	"time"
)

// singleNodeLeader builds a one-node "cluster" (no peers) already
// promoted to Leader — the simplest way to get real, actually-committed
// log entries without needing a live election or a second node to vote:
// a leader's own log trivially satisfies a majority of one, so Propose
// commits immediately (see advanceCommitIndexLocked's doc comment).
//
// Also starts RunApplyLoop and registers its shutdown via
// StopElectionTimer (shared stopCh) — Snapshot's contract is "the state
// machine has already applied through index," and Raft now actually
// enforces that (see Snapshot's own bounds check), so any test that's
// going to call Snapshot needs its apply loop actually running, not
// just entries sitting committed-but-unapplied in the log.
func singleNodeLeader(t *testing.T, persister Persister) *Raft {
	t.Helper()
	var r *Raft
	if persister != nil {
		r = NewRaftWithPersister(0, nil, NewFakeTransport(), persister)
	} else {
		r = NewRaft(0, nil, NewFakeTransport())
	}
	if err := r.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := r.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	go r.RunApplyLoop()
	t.Cleanup(r.StopElectionTimer)
	return r
}

// waitForLastAppliedAtLeast polls (via direct field access — this test
// file is white-box, same package as Raft itself) until r has actually
// APPLIED through index, not just committed it. Snapshot's contract is
// specifically about what's been applied; commitIndex alone isn't
// enough, since applyPending's ticker can legitimately lag a tick or
// two behind commitIndex advancing.
func waitForLastAppliedAtLeast(t *testing.T, r *Raft, index int, timeout time.Duration) {
	t.Helper()
	waitFor(t, timeout, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.lastApplied >= index
	})
}

// TestSnapshotDiscardsLogPrefix proves the headline behavior: after
// Snapshot(index, data), every log entry through index is gone from
// r.log, but the node's view of its own log — CommitIndex, its last
// log index/term, its ability to keep proposing — is unaffected.
func TestSnapshotDiscardsLogPrefix(t *testing.T) {
	r := singleNodeLeader(t, nil)

	var lastIndex int
	for i := 0; i < 5; i++ {
		index, _, ok := r.Propose("cmd")
		if !ok {
			t.Fatalf("Propose(%d): not leader", i)
		}
		lastIndex = index
	}
	if got := r.CommitIndex(); got != lastIndex {
		t.Fatalf("CommitIndex = %d, want %d (single-node cluster commits immediately)", got, lastIndex)
	}
	waitForLastAppliedAtLeast(t, r, 3, time.Second)

	if err := r.Snapshot(3, []byte("snapshot-through-3")); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	r.mu.Lock()
	logLen := len(r.log)
	lastIncluded := r.lastIncludedIndex
	r.mu.Unlock()
	if lastIncluded != 3 {
		t.Fatalf("lastIncludedIndex = %d, want 3", lastIncluded)
	}
	if wantLen := lastIndex - 3; logLen != wantLen {
		t.Fatalf("len(r.log) after compacting through 3 = %d, want %d (entries 4-%d)", logLen, wantLen, lastIndex)
	}

	// The node's own view of "what's committed" and "what's my last
	// entry" must be unaffected by the compaction — these describe
	// ABSOLUTE indices, not r.log's now-shorter physical length.
	if got := r.CommitIndex(); got != lastIndex {
		t.Fatalf("CommitIndex after Snapshot = %d, want %d (unchanged)", got, lastIndex)
	}

	// Proposing after a snapshot must still assign correct absolute
	// indices, not restart from r.log's new (shorter) length.
	newIndex, _, ok := r.Propose("cmd-after-snapshot")
	if !ok {
		t.Fatal("Propose after Snapshot: not leader")
	}
	if newIndex != lastIndex+1 {
		t.Fatalf("Propose after Snapshot returned index %d, want %d", newIndex, lastIndex+1)
	}
}

// TestSnapshotOfAlreadyCompactedIndexIsANoOp proves a stale/duplicate
// Snapshot call (index at or below what's already compacted) is
// harmless, not an error — the exact situation a slow caller racing a
// newer compaction, or a duplicate InstallSnapshot (Day 7), produces.
func TestSnapshotOfAlreadyCompactedIndexIsANoOp(t *testing.T) {
	r := singleNodeLeader(t, nil)
	for i := 0; i < 5; i++ {
		if _, _, ok := r.Propose("cmd"); !ok {
			t.Fatalf("Propose(%d): not leader", i)
		}
	}
	waitForLastAppliedAtLeast(t, r, 4, time.Second)
	if err := r.Snapshot(4, []byte("first")); err != nil {
		t.Fatalf("Snapshot(4): %v", err)
	}

	r.mu.Lock()
	logBefore := append([]LogEntry(nil), r.log...)
	r.mu.Unlock()

	if err := r.Snapshot(2, []byte("stale")); err != nil {
		t.Fatalf("Snapshot(2) (stale, index < lastIncludedIndex): %v", err)
	}
	if err := r.Snapshot(4, []byte("duplicate")); err != nil {
		t.Fatalf("Snapshot(4) (duplicate, index == lastIncludedIndex): %v", err)
	}

	r.mu.Lock()
	logAfter := append([]LogEntry(nil), r.log...)
	lastIncluded := r.lastIncludedIndex
	r.mu.Unlock()
	if lastIncluded != 4 {
		t.Fatalf("lastIncludedIndex = %d, want 4 (unchanged by the stale/duplicate calls)", lastIncluded)
	}
	if !reflect.DeepEqual(logAfter, logBefore) {
		t.Fatalf("log changed after a stale/duplicate Snapshot call: before %+v, after %+v", logBefore, logAfter)
	}
}

// TestSnapshotRejectsIndexPastLog proves Snapshot refuses to compact
// through an index this node hasn't even logged yet — a caller
// claiming to have applied something that was never proposed is a
// programmer error, not something to silently clamp.
func TestSnapshotRejectsIndexPastLog(t *testing.T) {
	r := singleNodeLeader(t, nil)
	if _, _, ok := r.Propose("cmd"); !ok {
		t.Fatal("Propose: not leader")
	}
	if err := r.Snapshot(100, []byte("bogus")); err == nil {
		t.Fatal("Snapshot(100) with only 1 log entry: want an error, got nil")
	}
}

// TestRaftStateSizeShrinksAfterSnapshot proves RaftStateSize — the
// quantity a size-based snapshot policy actually watches — reflects
// the compaction: it must shrink once the log's bulk is discarded, or
// a policy driven by it would just keep re-triggering forever.
func TestRaftStateSizeShrinksAfterSnapshot(t *testing.T) {
	r := singleNodeLeader(t, nil)
	for i := 0; i < 50; i++ {
		if _, _, ok := r.Propose("a reasonably sized command string to bulk up the log"); !ok {
			t.Fatalf("Propose(%d): not leader", i)
		}
	}
	before := r.RaftStateSize()

	waitForLastAppliedAtLeast(t, r, 45, time.Second)
	if err := r.Snapshot(45, []byte("snapshot")); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	after := r.RaftStateSize()

	if after >= before {
		t.Fatalf("RaftStateSize after compacting 45/50 entries = %d, want < %d (before)", after, before)
	}
}

// TestSnapshotStateSurvivesRestart is Day 6's version of Day 11's
// TestNewRaftWithPersisterRecoversState: lastIncludedIndex/Term, the
// now-shorter log, AND the snapshot bytes themselves must all come
// back correctly against a freshly constructed Raft sharing the same
// persister — the exact mechanism that simulates a crash-and-restart.
// Critically, commitIndex/lastApplied — normally volatile, reset to 0
// on every restart — must come back at least at lastIncludedIndex, not
// 0: index 1 doesn't exist in the restarted log at all any more.
func TestSnapshotStateSurvivesRestart(t *testing.T) {
	persister := NewMemoryPersister()
	r := singleNodeLeader(t, persister)

	var lastIndex int
	for i := 0; i < 6; i++ {
		index, _, ok := r.Propose("cmd")
		if !ok {
			t.Fatalf("Propose(%d): not leader", i)
		}
		lastIndex = index
	}
	waitForLastAppliedAtLeast(t, r, 4, time.Second)
	snapshotData := []byte("kv-store-state-as-of-index-4")
	if err := r.Snapshot(4, snapshotData); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// The "crash": discard r entirely, reconstruct against the same
	// persister — see persist_test.go's TestNewRaftWithPersisterRecoversState.
	restarted := NewRaftWithPersister(0, nil, NewFakeTransport(), persister)

	restarted.mu.Lock()
	gotLastIncludedIndex := restarted.lastIncludedIndex
	gotLastIncludedTerm := restarted.lastIncludedTerm
	gotLogLen := len(restarted.log)
	gotCommitIndex := restarted.commitIndex
	gotLastApplied := restarted.lastApplied
	restarted.mu.Unlock()

	if gotLastIncludedIndex != 4 {
		t.Fatalf("restarted lastIncludedIndex = %d, want 4", gotLastIncludedIndex)
	}
	if gotLastIncludedTerm != 1 {
		t.Fatalf("restarted lastIncludedTerm = %d, want 1 (this cluster never changed terms)", gotLastIncludedTerm)
	}
	if wantLogLen := lastIndex - 4; gotLogLen != wantLogLen {
		t.Fatalf("restarted log length = %d, want %d", gotLogLen, wantLogLen)
	}
	if gotCommitIndex < 4 {
		t.Fatalf("restarted commitIndex = %d, want >= 4 (lastIncludedIndex) — index 1 no longer exists to replay from", gotCommitIndex)
	}
	if gotLastApplied < 4 {
		t.Fatalf("restarted lastApplied = %d, want >= 4 (lastIncludedIndex)", gotLastApplied)
	}

	gotSnapshot := restarted.ReadSnapshot()
	if !reflect.DeepEqual(gotSnapshot, snapshotData) {
		t.Fatalf("restarted ReadSnapshot() = %q, want %q", gotSnapshot, snapshotData)
	}

	// And the restarted node must still be able to make progress: a
	// freshly proposed entry lands at the correct absolute index.
	if err := restarted.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := restarted.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	newIndex, _, ok := restarted.Propose("cmd-after-restart")
	if !ok {
		t.Fatal("Propose after restart: not leader")
	}
	if newIndex != lastIndex+1 {
		t.Fatalf("Propose after restart returned index %d, want %d", newIndex, lastIndex+1)
	}
}

// TestApplyLoopSkipsCompactedEntries proves the OTHER end of the
// pipeline still works after a snapshot: entries proposed AFTER the
// compaction point still arrive on ApplyCh, at the correct absolute
// index — applyPending's physicalIndexLocked translation has to be
// right, or this would either panic (out-of-range) or deliver the
// wrong entry's Command.
func TestApplyLoopSkipsCompactedEntries(t *testing.T) {
	r := singleNodeLeader(t, nil)

	for i := 0; i < 3; i++ {
		if _, _, ok := r.Propose("compacted-away"); !ok {
			t.Fatalf("Propose(%d): not leader", i)
		}
	}
	drainApplyCh(t, r, 3)

	if err := r.Snapshot(3, []byte("snap")); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	index, _, ok := r.Propose("survives-the-snapshot")
	if !ok {
		t.Fatal("Propose after Snapshot: not leader")
	}

	select {
	case msg := <-r.ApplyCh:
		if msg.Index != index || msg.Command != "survives-the-snapshot" {
			t.Fatalf("applied %+v, want Index=%d Command=%q", msg, index, "survives-the-snapshot")
		}
	case <-time.After(20 * ElectionTimeoutMax):
		t.Fatal("entry proposed after Snapshot was never applied")
	}
}

// TestSnapshotComposesWithRealReplication is the proof that matters
// most: compaction has to work in an actual multi-node cluster, not
// just on an isolated single-node leader. A 3-node cluster replicates
// several entries to full convergence, the leader snapshots through an
// early index, and the cluster must keep electing/replicating/
// committing normally afterward — proving the index-translation
// refactor across AppendEntries, replicateToPeer, and
// advanceCommitIndexLocked didn't break real cross-node agreement.
//
// This test deliberately keeps every follower caught up BEFORE
// snapshotting (via waitForAllCommitAtLeast), so nextIndex never falls
// at or below lastIncludedIndex for any peer — the case that needs
// Day 7's InstallSnapshot RPC, which doesn't exist yet. That gap is
// covered separately once Day 7 lands.
func TestSnapshotComposesWithRealReplication(t *testing.T) {
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

	leaderID := waitForSingleLeader(t, nodes, 20*ElectionTimeoutMax)

	var lastIndex int
	for i := 0; i < 5; i++ {
		index, _, ok := nodes[leaderID].Propose("cmd")
		if !ok {
			t.Fatalf("Propose(%d): not leader", i)
		}
		lastIndex = index
	}
	waitForAllCommitAtLeast(t, nodes, lastIndex, 20*ElectionTimeoutMax)
	// Snapshot's contract is "already applied," not just "committed" —
	// commitIndex advancing doesn't guarantee applyPending's ticker has
	// caught up yet (see waitForLastAppliedAtLeast's doc comment).
	waitForLastAppliedAtLeast(t, nodes[leaderID], 3, 20*ElectionTimeoutMax)

	if err := nodes[leaderID].Snapshot(3, []byte("snap-through-3")); err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// The cluster must still be able to make progress: a freshly
	// proposed entry replicates to every node and commits, exactly as
	// it would have before any compaction happened.
	newIndex, _, ok := nodes[leaderID].Propose("cmd-after-snapshot")
	if !ok {
		t.Fatal("Propose after Snapshot: not leader")
	}
	if newIndex != lastIndex+1 {
		t.Fatalf("Propose after Snapshot returned index %d, want %d", newIndex, lastIndex+1)
	}
	waitForAllCommitAtLeast(t, nodes, newIndex, 20*ElectionTimeoutMax)

	assertLogsConsistentAfterIndex(t, nodes, 3, newIndex)
}

// assertLogsConsistentAfterIndex is assertLogsConsistent's
// snapshot-aware counterpart: it compares each node's log only from
// lastIncludedIndex+1 onward (via termAtLocked, not a raw r.log slice,
// since r.log's physical layout differs across nodes that have and
// haven't compacted), through minCommit.
func assertLogsConsistentAfterIndex(t *testing.T, nodes map[int]*Raft, from, minCommit int) {
	t.Helper()
	for i := from + 1; i <= minCommit; i++ {
		var refTerm int
		refID := -1
		for id, n := range nodes {
			n.mu.Lock()
			term, ok := n.termAtLocked(i)
			n.mu.Unlock()
			if !ok {
				t.Fatalf("node %d has no term for committed index %d", id, i)
			}
			if refID == -1 {
				refTerm, refID = term, id
				continue
			}
			if term != refTerm {
				t.Fatalf("log mismatch at index %d: node %d has term %d, node %d has term %d", i, refID, refTerm, id, term)
			}
		}
	}
}

func drainApplyCh(t *testing.T, r *Raft, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case <-r.ApplyCh:
		case <-time.After(20 * ElectionTimeoutMax):
			t.Fatalf("timed out waiting for applied entry %d/%d", i+1, n)
		}
	}
}
