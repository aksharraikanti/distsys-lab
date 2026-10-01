package shardkv

import (
	"fmt"
	"math/bits"
	"sync"
	"time"
)

// OpKind identifies which KV operation a HistoryEntry recorded.
type OpKind int

const (
	OpGet OpKind = iota
	OpPut
	OpAppend
)

func (k OpKind) String() string {
	switch k {
	case OpGet:
		return "Get"
	case OpPut:
		return "Put"
	case OpAppend:
		return "Append"
	default:
		return fmt.Sprintf("OpKind(%d)", int(k))
	}
}

// HistoryEntry records one client operation as a [Invoke, Return) real-time
// interval together with its argument and result — Wing & Gong's own model
// of an operation, and the reason a plain before/after timestamp isn't
// enough: two operations invoked and returned in overlapping intervals are
// CONCURRENT, and a linearizable system is free to order them either way,
// not necessarily the order they happened to return in.
//
// Every entry here represents an operation this project's own clients
// (Clerk, ShardClerk, ...) have already reported as DEFINITELY succeeded —
// none of them ever return early on a transient failure, they retry until
// one actually commits (see ShardClerk's own doc comment). That sidesteps
// the harder "maybe it happened, maybe it didn't" variant of linearizability
// checking real systems with timeouts need: every recorded entry here must
// be explained by SOME linearization, none of them is allowed to be dropped.
type HistoryEntry struct {
	ClientID int64
	Key      string
	Kind     OpKind
	Arg      string // Put/Append's value; unused for Get
	Result   string // Get's returned value; unused for Put/Append

	Invoke time.Time
	Return time.Time
}

// History is a recorder multiple client goroutines append completed
// operations to concurrently — the raw material IsLinearizable checks.
type History struct {
	mu      sync.Mutex
	entries []HistoryEntry
}

// NewHistory returns an empty History ready to record.
func NewHistory() *History { return &History{} }

// Record appends e. Safe for concurrent use — recording real concurrent
// client traffic is the entire point.
func (h *History) Record(e HistoryEntry) {
	h.mu.Lock()
	h.entries = append(h.entries, e)
	h.mu.Unlock()
}

// Entries returns a snapshot of everything recorded so far.
func (h *History) Entries() []HistoryEntry {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]HistoryEntry, len(h.entries))
	copy(out, h.entries)
	return out
}

// RecordingShardClerk wraps a ShardClerk, timestamping every call into a
// shared History. Like the ShardClerk it wraps, a single
// RecordingShardClerk is NOT safe for concurrent use — one per simulated
// client goroutine, each wrapping its own ShardClerk, all recording into
// the SAME shared History (History itself is the thing built to be shared).
type RecordingShardClerk struct {
	ck       *ShardClerk
	h        *History
	clientID int64
}

// NewRecordingShardClerk returns a RecordingShardClerk that records every
// call it makes through ck into h, labeled with clientID (purely for a
// human reading a failed history back later — the checker itself doesn't
// care which client did what, only real-time order and per-key effects).
func NewRecordingShardClerk(ck *ShardClerk, h *History, clientID int64) *RecordingShardClerk {
	return &RecordingShardClerk{ck: ck, h: h, clientID: clientID}
}

func (r *RecordingShardClerk) Get(key string) string {
	invoke := time.Now()
	value := r.ck.Get(key)
	r.h.Record(HistoryEntry{ClientID: r.clientID, Key: key, Kind: OpGet, Result: value, Invoke: invoke, Return: time.Now()})
	return value
}

func (r *RecordingShardClerk) Put(key, value string) {
	invoke := time.Now()
	r.ck.Put(key, value)
	r.h.Record(HistoryEntry{ClientID: r.clientID, Key: key, Kind: OpPut, Arg: value, Invoke: invoke, Return: time.Now()})
}

func (r *RecordingShardClerk) Append(key, value string) {
	invoke := time.Now()
	r.ck.Append(key, value)
	r.h.Record(HistoryEntry{ClientID: r.clientID, Key: key, Kind: OpAppend, Arg: value, Invoke: invoke, Return: time.Now()})
}

// IsLinearizable reports whether entries is explainable by SOME total order
// consistent with real time, under this project's actual single-key
// semantics: Put replaces the value, Append concatenates onto it, Get
// returns the current value, and a never-written key starts at "".
//
// Checked independently PER KEY — sound, not just convenient, because
// operations on different keys can never affect each other in this stage
// (no multi-key transactions before Stage 7's distributed transactions): a
// global linearization exists if and only if an independent one does for
// every key, so the search space is the product of much smaller per-key
// searches rather than one search over the whole history at once.
func IsLinearizable(entries []HistoryEntry) bool {
	byKey := make(map[string][]HistoryEntry)
	for _, e := range entries {
		byKey[e.Key] = append(byKey[e.Key], e)
	}
	for _, es := range byKey {
		if !linearizableForKey(es) {
			return false
		}
	}
	return true
}

// linearizableSearchState is memoized across the backtracking search below:
// the same (which operations are already placed, what the register's value
// is after placing them) combination is only ever worth exploring once —
// without this, the search is O(n!) in the number of operations on the key,
// since every permutation consistent with real time would be tried from
// scratch.
type linearizableSearchState struct {
	used  uint64 // bitmask of entries already placed in the trial order
	value string // the register's value after everything placed so far
}

// linearizableForKey runs Wing & Gong's own search, restricted to one key:
// find SOME total order of entries, consistent with each operation's real-
// time interval, under which every Get's recorded Result matches the
// register's value at that point in the order.
func linearizableForKey(entries []HistoryEntry) bool {
	if len(entries) > 63 {
		// Every history this project actually generates is small (a
		// handful of clients, each a handful of operations); 63 operations
		// on a SINGLE key in one test run would itself be a sign something
		// about the test's scope is wrong, not a real limit worth lifting.
		panic(fmt.Sprintf("shardkv: IsLinearizable supports at most 63 operations per key, got %d", len(entries)))
	}
	memo := make(map[linearizableSearchState]bool)
	return linearizableSearch(entries, 0, "", memo)
}

// linearizableSearch tries to extend a partial linearization (used, value)
// to a complete one. Returns true the moment any complete, valid
// linearization is found; false once every option from this state has been
// tried and failed (memoized, so a dead end is never re-explored).
func linearizableSearch(entries []HistoryEntry, used uint64, value string, memo map[linearizableSearchState]bool) bool {
	if bits.OnesCount64(used) == len(entries) {
		return true
	}
	state := linearizableSearchState{used: used, value: value}
	if memo[state] {
		return false
	}

	for i, e := range entries {
		bit := uint64(1) << i
		if used&bit != 0 {
			continue
		}
		if !linearizableReadyNext(entries, used, i) {
			continue // some other unplaced op is forced before this one
		}

		nextValue := value
		resultOK := true
		switch e.Kind {
		case OpGet:
			resultOK = e.Result == value
		case OpPut:
			nextValue = e.Arg
		case OpAppend:
			nextValue = value + e.Arg
		}
		if resultOK && linearizableSearch(entries, used|bit, nextValue, memo) {
			return true
		}
	}

	memo[state] = true
	return false
}

// linearizableReadyNext reports whether entries[i] is legal to place next:
// no OTHER not-yet-placed entry's Return already precedes entries[i]'s
// Invoke, since real time would force that entry first. Anything not ruled
// out this way is a legal candidate to TRY (not a guarantee it leads
// anywhere) — operations with overlapping, concurrent intervals are exactly
// the ones with no such constraint between them, free to linearize either
// way.
func linearizableReadyNext(entries []HistoryEntry, used uint64, i int) bool {
	for j := range entries {
		bit := uint64(1) << j
		if j == i || used&bit != 0 {
			continue
		}
		if happensBeforeInHistory(entries[j], entries[i]) {
			return false
		}
	}
	return true
}

// happensBeforeInHistory reports whether a's interval definitely ended
// before b's began — real time leaves no other possibility, since a
// returned at or before the instant b was invoked.
func happensBeforeInHistory(a, b HistoryEntry) bool {
	return !a.Return.After(b.Invoke)
}
