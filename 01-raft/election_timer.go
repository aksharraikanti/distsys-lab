package raft

import "time"

// randomElectionTimeout returns a value uniformly distributed in
// [ElectionTimeoutMin, ElectionTimeoutMax). Randomizing per node — not a
// single fixed timeout — is the core of Raft's split-vote avoidance: if
// every node used the same timeout, they'd all become candidates at the
// same instant on every leader failure and split every election forever
// (Raft paper §5.2).
func (r *Raft) randomElectionTimeout() time.Duration {
	r.rngMu.Lock()
	defer r.rngMu.Unlock()

	span := int64(ElectionTimeoutMax - ElectionTimeoutMin)
	return ElectionTimeoutMin + time.Duration(r.rng.Int63n(span))
}

// RunElectionTimer runs the election-timeout loop until StopElectionTimer
// is called. It blocks, so the caller runs it in its own goroutine:
// `go node.RunElectionTimer()`.
//
// Each iteration waits for one of three things: the randomized timeout
// elapsing (the node starts an election), a reset signal arriving (a
// heartbeat or granted vote from a legitimate leader — wired in by later
// days, restarts the countdown with a freshly chosen timeout), or a stop
// signal (the loop exits).
func (r *Raft) RunElectionTimer() {
	timer := time.NewTimer(r.randomElectionTimeout())
	defer timer.Stop()

	for {
		select {
		case <-r.stopCh:
			return

		case <-r.resetElectionTimer:
			if !timer.Stop() {
				<-timer.C // drain: the timer had already fired concurrently
			}
			timer.Reset(r.randomElectionTimeout())

		case <-timer.C:
			// A Leader doesn't run against its own election — it's not
			// waiting to hear from anyone, so a fired timer here just
			// means restart the countdown and keep leading.
			if r.State() != Leader {
				_ = r.BecomeCandidate()
			}
			timer.Reset(r.randomElectionTimeout())
		}
	}
}

// ResetElectionTimer restarts the running election timer's countdown.
// Non-blocking: if the timer goroutine isn't running yet, or a reset is
// already pending, the signal is safely dropped — a dropped reset just
// makes the current countdown run a little longer, which never causes an
// incorrect election (only, at worst, a slightly later one).
func (r *Raft) ResetElectionTimer() {
	select {
	case r.resetElectionTimer <- struct{}{}:
	default:
	}
}

// StopElectionTimer stops the running RunElectionTimer loop. Safe to call
// multiple times, and safe to call even if RunElectionTimer was never
// started.
func (r *Raft) StopElectionTimer() {
	r.stopOnce.Do(func() {
		close(r.stopCh)
	})
}
