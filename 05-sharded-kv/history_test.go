package shardkv

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// ts returns a synthetic, strictly-ordered instant — hand-built histories
// use these instead of real time.Now()/sleeps so the exact real-time
// relationships a test depends on (concurrent vs. definitely-ordered) are
// exact and not at the mercy of scheduling jitter.
func ts(n int64) time.Time { return time.Unix(0, n) }

// TestIsLinearizableAcceptsAnObviouslyValidHistory is the sanity check
// before any of the adversarial ones: two operations with no real
// concurrency at all (one fully precedes the other) that are already
// consistent with each other must be accepted.
func TestIsLinearizableAcceptsAnObviouslyValidHistory(t *testing.T) {
	h := []HistoryEntry{
		{ClientID: 1, Key: "x", Kind: OpPut, Arg: "1", Invoke: ts(0), Return: ts(1)},
		{ClientID: 2, Key: "x", Kind: OpGet, Result: "1", Invoke: ts(2), Return: ts(3)},
	}
	if !IsLinearizable(h) {
		t.Fatal("an obviously valid, non-concurrent history was rejected")
	}
}

// TestIsLinearizableAcceptsEitherOrderOfConcurrentAppends checks the search
// actually explores reorderings, not just the order entries happened to be
// recorded in: two Appends with genuinely OVERLAPPING intervals (real
// concurrency, no real-time constraint between them) may linearize in
// either order, so a Get afterward seeing EITHER concatenation must be
// accepted.
func TestIsLinearizableAcceptsEitherOrderOfConcurrentAppends(t *testing.T) {
	concurrentOrder1 := []HistoryEntry{
		{ClientID: 1, Key: "x", Kind: OpAppend, Arg: "a", Invoke: ts(0), Return: ts(10)},
		{ClientID: 2, Key: "x", Kind: OpAppend, Arg: "b", Invoke: ts(1), Return: ts(2)}, // nested inside #1's interval
		{ClientID: 3, Key: "x", Kind: OpGet, Result: "ab", Invoke: ts(11), Return: ts(12)},
	}
	if !IsLinearizable(concurrentOrder1) {
		t.Fatal("a Get matching one valid order of two concurrent Appends was rejected")
	}

	concurrentOrder2 := []HistoryEntry{
		{ClientID: 1, Key: "x", Kind: OpAppend, Arg: "a", Invoke: ts(0), Return: ts(10)},
		{ClientID: 2, Key: "x", Kind: OpAppend, Arg: "b", Invoke: ts(1), Return: ts(2)},
		{ClientID: 3, Key: "x", Kind: OpGet, Result: "ba", Invoke: ts(11), Return: ts(12)},
	}
	if !IsLinearizable(concurrentOrder2) {
		t.Fatal("a Get matching the OTHER valid order of two concurrent Appends was rejected")
	}
}

// TestIsLinearizableRejectsStaleRead: a Put completes, strictly before (not
// concurrently with) a later Get — real time leaves no other possibility —
// yet the Get reports the OLD value. No linearization can explain this: the
// Put MUST precede the Get in any valid order, so the Get MUST see it.
func TestIsLinearizableRejectsStaleRead(t *testing.T) {
	h := []HistoryEntry{
		{ClientID: 1, Key: "x", Kind: OpPut, Arg: "1", Invoke: ts(0), Return: ts(1)},
		{ClientID: 2, Key: "x", Kind: OpGet, Result: "", Invoke: ts(2), Return: ts(3)},
	}
	if IsLinearizable(h) {
		t.Fatal("a stale read (Get missing a write that definitely happened before it) was accepted")
	}
}

// TestIsLinearizableRejectsLostWrite: three operations, none concurrent
// with any other (A before B before C, each interval fully disjoint), so
// there is exactly ONE possible order. A Get after two Appends must see
// BOTH — seeing only the second one means the first was silently lost.
func TestIsLinearizableRejectsLostWrite(t *testing.T) {
	h := []HistoryEntry{
		{ClientID: 1, Key: "x", Kind: OpAppend, Arg: "1", Invoke: ts(0), Return: ts(1)},
		{ClientID: 2, Key: "x", Kind: OpAppend, Arg: "2", Invoke: ts(2), Return: ts(3)},
		{ClientID: 3, Key: "x", Kind: OpGet, Result: "2", Invoke: ts(4), Return: ts(5)},
	}
	if IsLinearizable(h) {
		t.Fatal("a lost write (Get missing an earlier Append with no concurrency to excuse it) was accepted")
	}
}

// TestIsLinearizableRejectsDuplicateApply: a single Append, then a Get that
// sees its effect TWICE. There is only one Append in the whole history —
// nothing could make its value appear twice in any linearization.
func TestIsLinearizableRejectsDuplicateApply(t *testing.T) {
	h := []HistoryEntry{
		{ClientID: 1, Key: "x", Kind: OpAppend, Arg: "1", Invoke: ts(0), Return: ts(1)},
		{ClientID: 2, Key: "x", Kind: OpGet, Result: "11", Invoke: ts(2), Return: ts(3)},
	}
	if IsLinearizable(h) {
		t.Fatal("a duplicate apply (Get seeing one Append's effect twice) was accepted")
	}
}

// TestIsLinearizableChecksPerKeyIndependently: a bad history on key "y"
// must be caught even while key "x" alone is perfectly fine — the per-key
// split can't let one key's problem hide behind another's correctness.
func TestIsLinearizableChecksPerKeyIndependently(t *testing.T) {
	h := []HistoryEntry{
		{ClientID: 1, Key: "x", Kind: OpPut, Arg: "1", Invoke: ts(0), Return: ts(1)},
		{ClientID: 1, Key: "x", Kind: OpGet, Result: "1", Invoke: ts(2), Return: ts(3)},
		{ClientID: 2, Key: "y", Kind: OpPut, Arg: "a", Invoke: ts(0), Return: ts(1)},
		{ClientID: 2, Key: "y", Kind: OpGet, Result: "WRONG", Invoke: ts(2), Return: ts(3)},
	}
	if IsLinearizable(h) {
		t.Fatal("a bad history on one key must be caught even when another key's history is fine")
	}
}

// TestIsLinearizableCatchesACrossClientAnomaly is the exact capability
// TASKS.md says the private-key-per-client trick cannot provide: two
// DIFFERENT clients writing the SAME key, interleaved in a way no
// linearization can explain — client 2's Get, which starts only after
// client 1's Put returns, reports a value that includes neither client's
// write.
func TestIsLinearizableCatchesACrossClientAnomaly(t *testing.T) {
	h := []HistoryEntry{
		{ClientID: 1, Key: "shared", Kind: OpPut, Arg: "from-1", Invoke: ts(0), Return: ts(1)},
		{ClientID: 2, Key: "shared", Kind: OpGet, Result: "neither-clients-value", Invoke: ts(2), Return: ts(3)},
	}
	if IsLinearizable(h) {
		t.Fatal("a cross-client anomaly (a shared key's Get matching no possible writer) was accepted")
	}
}

// TestConcurrentClientsOnSharedKeysProduceALinearizableHistory is Day 7's
// own demonstration, not just a unit test of the checker in isolation: real
// concurrent clients, through the real GroupServer/ShardClerk stack and a
// live reconfiguration, writing to a SMALL SHARED set of keys (not one
// private key per client, which is exactly what every earlier stress test
// in this project used and exactly what TASKS.md says can't catch a
// cross-client anomaly). If this project's actual implementation has one,
// this is where it would show up.
func TestConcurrentClientsOnSharedKeysProduceALinearizableHistory(t *testing.T) {
	_, ctrlers, _, ctrlerCleanup := newTestCtrlerCluster(3)
	defer ctrlerCleanup()
	admin := NewCtrlerClerk(ctrlers)

	nodes1, g1, _, g1Cleanup := newTestGroupCluster(3, 1, ctrlers)
	defer g1Cleanup()
	_, g2, _, g2Cleanup := newTestGroupCluster(3, 2, ctrlers)
	defer g2Cleanup()
	groups := map[int][]*GroupServer{1: g1, 2: g2}
	wirePeers(groups)

	admin.Join(map[int][]string{1: addrsFor(1)})
	waitForGroupConfig(t, g1[waitForGroupLeader(t, nodes1, 2*time.Second)], 1, 2*time.Second)

	const numClients = 4
	const opsPerClient = 15
	sharedKeys := []string{"shared-a", "shared-b"}

	history := NewHistory()
	var wg sync.WaitGroup
	for i := 0; i < numClients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ck := NewRecordingShardClerk(NewShardClerk(NewCtrlerClerk(ctrlers), groups), history, int64(i))
			for j := 0; j < opsPerClient; j++ {
				key := sharedKeys[j%len(sharedKeys)]
				switch j % 3 {
				case 0:
					ck.Put(key, fmt.Sprintf("v%d-%d", i, j))
				case 1:
					ck.Append(key, fmt.Sprintf("[%d-%d]", i, j))
				case 2:
					ck.Get(key)
				}
			}
		}(i)
	}

	go func() {
		time.Sleep(3 * time.Millisecond)
		admin.Join(map[int][]string{2: addrsFor(2)})
		time.Sleep(5 * time.Millisecond)
		admin.Move(Key2Shard(sharedKeys[0]), 2)
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("clients never finished through reconfiguration")
	}

	entries := history.Entries()
	if len(entries) != numClients*opsPerClient {
		t.Fatalf("recorded %d history entries, want %d", len(entries), numClients*opsPerClient)
	}
	if !IsLinearizable(entries) {
		t.Fatal("concurrent clients on shared keys through live reconfiguration produced a NON-linearizable history")
	}
}
