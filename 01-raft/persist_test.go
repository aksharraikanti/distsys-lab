package raft

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// TestMemoryPersisterRoundTrips proves the Persister itself is honest
// before trusting Raft's use of it: whatever bytes go in via SaveState
// come back unchanged via ReadState, and a persister nothing has ever
// been saved to reads back as empty (a first-ever boot), not an error.
func TestMemoryPersisterRoundTrips(t *testing.T) {
	p := NewMemoryPersister()

	data, err := p.ReadState()
	if err != nil {
		t.Fatalf("ReadState on a never-written persister: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("ReadState on a never-written persister = %v, want empty", data)
	}

	want := []byte("hello raft")
	if err := p.SaveState(want); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	got, err := p.ReadState()
	if err != nil {
		t.Fatalf("ReadState: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ReadState = %v, want %v", got, want)
	}
}

// TestMemoryPersisterSnapshotRoundTrips proves SaveStateAndSnapshot's
// snapshot half round-trips independently of SaveState's own state
// blob — the two are stored (and read back) separately, not folded
// together.
func TestMemoryPersisterSnapshotRoundTrips(t *testing.T) {
	p := NewMemoryPersister()

	snap, err := p.ReadSnapshot()
	if err != nil {
		t.Fatalf("ReadSnapshot on a never-written persister: %v", err)
	}
	if len(snap) != 0 {
		t.Fatalf("ReadSnapshot on a never-written persister = %v, want empty", snap)
	}

	wantState := []byte("state-blob")
	wantSnapshot := []byte("snapshot-blob")
	if err := p.SaveStateAndSnapshot(wantState, wantSnapshot); err != nil {
		t.Fatalf("SaveStateAndSnapshot: %v", err)
	}

	gotState, err := p.ReadState()
	if err != nil {
		t.Fatalf("ReadState: %v", err)
	}
	if !reflect.DeepEqual(gotState, wantState) {
		t.Fatalf("ReadState = %v, want %v", gotState, wantState)
	}
	gotSnapshot, err := p.ReadSnapshot()
	if err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}
	if !reflect.DeepEqual(gotSnapshot, wantSnapshot) {
		t.Fatalf("ReadSnapshot = %v, want %v", gotSnapshot, wantSnapshot)
	}
}

// TestFilePersisterRoundTrips is the same proof against a real file on
// disk — including that a second SaveState correctly overwrites the
// first (the atomic-rename path, not just the initial write).
func TestFilePersisterRoundTrips(t *testing.T) {
	dir := t.TempDir()
	p := NewFilePersister(filepath.Join(dir, "state.bin"))

	data, err := p.ReadState()
	if err != nil {
		t.Fatalf("ReadState before any SaveState: %v", err)
	}
	if len(data) != 0 {
		t.Fatalf("ReadState before any SaveState = %v, want empty (first-ever boot)", data)
	}

	if err := p.SaveState([]byte("version 1")); err != nil {
		t.Fatalf("SaveState (1): %v", err)
	}
	if err := p.SaveState([]byte("version 2, longer than the first")); err != nil {
		t.Fatalf("SaveState (2): %v", err)
	}

	got, err := p.ReadState()
	if err != nil {
		t.Fatalf("ReadState: %v", err)
	}
	if string(got) != "version 2, longer than the first" {
		t.Fatalf("ReadState = %q, want the second SaveState's content", got)
	}

	// No stray temp files should be left behind after a successful save.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("directory has %d entries after 2 saves, want exactly 1 (no leftover temp files): %v", len(entries), entries)
	}
}

// TestFilePersisterSnapshotRoundTrips is the on-disk equivalent of
// TestMemoryPersisterSnapshotRoundTrips: SaveStateAndSnapshot writes
// both blobs to separate files (state.bin and state.bin.snapshot), and
// each reads back independently, with no leftover temp files from
// either write.
func TestFilePersisterSnapshotRoundTrips(t *testing.T) {
	dir := t.TempDir()
	p := NewFilePersister(filepath.Join(dir, "state.bin"))

	if err := p.SaveState([]byte("plain state, no snapshot yet")); err != nil {
		t.Fatalf("SaveState: %v", err)
	}
	if err := p.SaveStateAndSnapshot([]byte("state-with-snapshot"), []byte("snapshot-bytes")); err != nil {
		t.Fatalf("SaveStateAndSnapshot: %v", err)
	}

	gotState, err := p.ReadState()
	if err != nil {
		t.Fatalf("ReadState: %v", err)
	}
	if string(gotState) != "state-with-snapshot" {
		t.Fatalf("ReadState = %q, want %q", gotState, "state-with-snapshot")
	}
	gotSnapshot, err := p.ReadSnapshot()
	if err != nil {
		t.Fatalf("ReadSnapshot: %v", err)
	}
	if string(gotSnapshot) != "snapshot-bytes" {
		t.Fatalf("ReadSnapshot = %q, want %q", gotSnapshot, "snapshot-bytes")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("directory has %d entries after SaveState + SaveStateAndSnapshot, want exactly 2 (state.bin, state.bin.snapshot — no leftover temp files): %v", len(entries), entries)
	}
}

// TestNewRaftWithPersisterRecoversState is Day 11's headline proof: a
// node's currentTerm, votedFor, and log survive being discarded and
// reconstructed against the SAME persister — the exact mechanism that
// simulates a crash-and-restart without a real process exit.
func TestNewRaftWithPersisterRecoversState(t *testing.T) {
	persister := NewMemoryPersister()
	transport := NewFakeTransport()

	r := NewRaftWithPersister(0, []int{1, 2}, transport, persister)
	if err := r.BecomeCandidate(); err != nil { // term -> 1, votedFor -> 0
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := r.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	if _, _, ok := r.Propose("cmd-1"); !ok {
		t.Fatal("Propose should succeed on the leader")
	}
	if _, _, ok := r.Propose("cmd-2"); !ok {
		t.Fatal("Propose should succeed on the leader")
	}

	wantTerm := r.Term()
	r.mu.Lock()
	wantVotedFor := r.votedFor
	wantLog := append([]LogEntry(nil), r.log...)
	r.mu.Unlock()

	// The "crash": discard r entirely, never call StopElectionTimer or
	// touch it again. The only thing that survives is the persister.
	restarted := NewRaftWithPersister(0, []int{1, 2}, transport, persister)

	// Every restarted node comes back as a Follower regardless of what it
	// was before crashing (Raft paper: state is NOT persistent) — only
	// term/votedFor/log are recovered.
	if got := restarted.State(); got != Follower {
		t.Fatalf("restarted node state = %s, want Follower (state is never persisted)", got)
	}
	if got := restarted.Term(); got != wantTerm {
		t.Fatalf("restarted node term = %d, want %d", got, wantTerm)
	}
	restarted.mu.Lock()
	gotVotedFor := restarted.votedFor
	gotLog := append([]LogEntry(nil), restarted.log...)
	restarted.mu.Unlock()
	if gotVotedFor != wantVotedFor {
		t.Fatalf("restarted node votedFor = %d, want %d", gotVotedFor, wantVotedFor)
	}
	if !reflect.DeepEqual(gotLog, wantLog) {
		t.Fatalf("restarted node log = %+v, want %+v", gotLog, wantLog)
	}
}

// TestNewRaftWithPersisterFirstBootIsFresh proves a persister that's
// never had anything saved to it (a genuine first-ever boot, not a
// restart) leaves a node at its normal defaults — persistence must never
// invent state that was never actually written.
func TestNewRaftWithPersisterFirstBootIsFresh(t *testing.T) {
	persister := NewMemoryPersister()
	r := NewRaftWithPersister(0, []int{1, 2}, NewFakeTransport(), persister)

	if got := r.State(); got != Follower {
		t.Fatalf("fresh node state = %s, want Follower", got)
	}
	if got := r.Term(); got != 0 {
		t.Fatalf("fresh node term = %d, want 0", got)
	}
	r.mu.Lock()
	votedFor := r.votedFor
	logLen := len(r.log)
	r.mu.Unlock()
	if votedFor != -1 {
		t.Fatalf("fresh node votedFor = %d, want -1", votedFor)
	}
	if logLen != 0 {
		t.Fatalf("fresh node log length = %d, want 0", logLen)
	}
}

// TestGrantedVoteSurvivesRestart is the specific safety gap Day 6's
// TestNodeRestartRejoinsCluster explicitly flagged as open: a restarted
// node must remember who it already voted for this term, or it could
// vote for a second, conflicting candidate in the same term — a real
// safety violation, not just a liveness hiccup. This is what Day 11
// closes.
func TestGrantedVoteSurvivesRestart(t *testing.T) {
	persister := NewMemoryPersister()
	transport := NewFakeTransport()
	r := NewRaftWithPersister(0, []int{1, 2}, transport, persister)

	var reply RequestVoteReply
	if err := r.RequestVote(&RequestVoteArgs{Term: 1, CandidateID: 5}, &reply); err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if !reply.VoteGranted {
		t.Fatal("expected the first vote request to be granted")
	}

	// "Crash" and restart against the same persister.
	restarted := NewRaftWithPersister(0, []int{1, 2}, transport, persister)

	// A DIFFERENT candidate asking for the same term must still be
	// refused — the restarted node remembers it already voted for
	// candidate 5, even though that memory lived only in the persister,
	// not in the (discarded) old in-memory struct.
	var reply2 RequestVoteReply
	if err := restarted.RequestVote(&RequestVoteArgs{Term: 1, CandidateID: 6}, &reply2); err != nil {
		t.Fatalf("RequestVote (post-restart): %v", err)
	}
	if reply2.VoteGranted {
		t.Fatal("expected the restarted node to refuse a second candidate for a term it already voted in — this is the exact safety gap Day 11 exists to close")
	}
}
