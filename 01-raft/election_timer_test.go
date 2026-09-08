package raft

import (
	"testing"
	"time"
)

// waitFor polls condition until it's true or timeout elapses, failing the
// test if it never becomes true. Used instead of a single fixed sleep so
// these tests aren't flaky under machine load — they succeed as soon as
// the condition is met rather than needing to guess the exact right sleep
// duration.
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

func TestRandomElectionTimeoutInRange(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())

	for i := 0; i < 1000; i++ {
		d := r.randomElectionTimeout()
		if d < ElectionTimeoutMin || d >= ElectionTimeoutMax {
			t.Fatalf("randomElectionTimeout() = %s, want in [%s, %s)", d, ElectionTimeoutMin, ElectionTimeoutMax)
		}
	}
}

// TestElectionTimeoutTriggersCandidate proves Day 3's core requirement:
// with nothing resetting the timer, a Follower becomes a Candidate once
// the election timeout elapses.
func TestElectionTimeoutTriggersCandidate(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	go r.RunElectionTimer()
	defer r.StopElectionTimer()

	waitFor(t, 5*ElectionTimeoutMax, func() bool {
		return r.State() == Candidate
	})
}

// TestResetElectionTimerPreventsTimeout proves a node that keeps hearing
// from a leader (simulated here by repeatedly calling ResetElectionTimer,
// standing in for the heartbeats Day 5 will wire up) never starts an
// election.
func TestResetElectionTimerPreventsTimeout(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	go r.RunElectionTimer()
	defer r.StopElectionTimer()

	stopResets := make(chan struct{})
	go func() {
		ticker := time.NewTicker(ElectionTimeoutMin / 2)
		defer ticker.Stop()
		for {
			select {
			case <-stopResets:
				return
			case <-ticker.C:
				r.ResetElectionTimer()
			}
		}
	}()

	time.Sleep(5 * ElectionTimeoutMax)
	close(stopResets)

	if got := r.State(); got != Follower {
		t.Fatalf("state after sustained resets = %s, want Follower (should never have timed out)", got)
	}
}

// TestLeaderDoesNotReElect proves a node already in the Leader state
// ignores its own election timeout instead of trying (and failing) to
// nominate itself Candidate — see the State() != Leader guard in
// RunElectionTimer.
func TestLeaderDoesNotReElect(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	if err := r.BecomeCandidate(); err != nil {
		t.Fatalf("BecomeCandidate: %v", err)
	}
	if err := r.BecomeLeader(); err != nil {
		t.Fatalf("BecomeLeader: %v", err)
	}
	leaderTerm := r.Term()

	go r.RunElectionTimer()
	defer r.StopElectionTimer()

	time.Sleep(5 * ElectionTimeoutMax)

	if got := r.State(); got != Leader {
		t.Fatalf("state after timeout while Leader = %s, want unchanged Leader", got)
	}
	if got := r.Term(); got != leaderTerm {
		t.Fatalf("term after timeout while Leader = %d, want unchanged %d", got, leaderTerm)
	}
}

// TestStopElectionTimerIsIdempotent proves StopElectionTimer can be
// called more than once (and before RunElectionTimer starts) without
// panicking — sync.Once is what makes this safe.
func TestStopElectionTimerIsIdempotent(t *testing.T) {
	r := NewRaft(0, []int{1, 2}, NewFakeTransport())
	r.StopElectionTimer()
	r.StopElectionTimer()

	done := make(chan struct{})
	go func() {
		r.RunElectionTimer() // must return immediately: stopCh is already closed
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunElectionTimer did not exit promptly after StopElectionTimer was called first")
	}
}
