package kvstore

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
)

// commitTimeout bounds how long a client-facing RPC handler waits for
// its proposed command to actually commit before giving up and telling
// the client to retry elsewhere. Raft never guarantees a Proposed entry
// commits — this node can lose leadership before a majority confirms it
// (see PutAppend) — so waiting forever isn't an option.
//
// leaderCheckInterval is how often PutAppend polls whether it's still
// leader of the term it Proposed in, while waiting for its entry to
// commit (Day 4). This is the FAST path out of a doomed wait: if this
// node loses leadership, its still-uncommitted entry can be silently
// overwritten by whoever becomes leader next, and nothing will ever
// arrive on the notify channel to say so — there's no guarantee ANY
// future entry lands at that exact index again. Waiting for the full
// commitTimeout in that case would be needlessly slow when the node
// already knows, almost immediately, that it's no longer in a position
// to get this entry committed.
const (
	commitTimeout       = 20 * raft.ElectionTimeoutMax
	leaderCheckInterval = raft.HeartbeatInterval
)

// KVServer is a key-value state machine driven by a Raft node's
// committed log. Get and PutAppend are real client-facing operations; a
// retried PutAppend (same ClientID+SeqNum landing at a new log index)
// has its effect applied at most once (Day 3); and a PutAppend whose
// proposal is superseded — either by a different entry landing at the
// same index, or by this node losing leadership before anything lands
// there at all — is detected and reported rather than hanging or
// falsely reporting success (Day 4).
type KVServer struct {
	mu    sync.Mutex
	rf    *raft.Raft
	store map[string]string

	// notifyChans lets a PutAppend call learn, without polling, exactly
	// which Op landed at the log index it Proposed — see PutAppend and
	// applyLoop. Keyed by log index; each channel is buffered(1) so
	// applyLoop's send never blocks even if the waiter already gave up
	// on a timeout.
	notifyChans map[int]chan Op

	// duplicateTable[clientID] is the highest SeqNum from that client
	// whose effect has already been applied. Tracked here, in the state
	// machine, not in the RPC handler layer — every replica applies the
	// same committed log in the same order, so every replica computes
	// the identical answer to "have I seen this one before," which is
	// what makes the dedup decision itself replicated and consistent
	// rather than a per-node guess. Living in the state machine is also
	// what lets this table survive a snapshot (Day 6): it's serialized
	// into the snapshot right alongside store, so a node that restores
	// from one doesn't forget which requests it's already seen.
	duplicateTable map[int64]int64

	// maxRaftState bounds how large rf's persisted currentTerm/votedFor/
	// log (raft.Raft.RaftStateSize) is allowed to grow before applyLoop
	// snapshots — the Raft log otherwise grows forever, since nothing
	// before Day 6 ever consumed committed entries into compactable
	// state. -1 disables snapshotting entirely, which is what every
	// pre-Day-6 test still wants: it isn't testing compaction, and
	// wiring a threshold into it would risk snapshotting mid-assertion
	// for no reason relevant to what it's actually checking.
	maxRaftState int

	// noopAppliedTerm is the term of the most recent no-op entry this
	// node's applyLoop has applied (0 = none yet). Get refuses to answer
	// unless it equals rf's current term: that's Raft §8's rule that a
	// leader must have committed an entry from its OWN term before it may
	// serve reads. Until then, entries a previous leader already
	// committed may exist that this node hasn't applied yet, and a read
	// would silently miss them.
	noopAppliedTerm int

	// stopCh/stopOnce shut down applyLoop and noopLoop. Day 5's own
	// stress test is what surfaced why this needs to be real rather
	// than deferred: a Go test binary runs every test in one process,
	// and an un-stoppable noopLoop ticking forever on an orphaned
	// KVServer from an EARLIER test — one whose underlying Raft node
	// happened to still be Leader when that test ended — keeps
	// proposing no-ops against it indefinitely. Enough of those
	// accumulate across a test run to visibly contend for scheduler
	// time and introduce exactly the kind of timing flakiness this
	// project has already had to fix once in 01-raft. Stop closes this
	// for real, the same idempotent-via-sync.Once shape raft.Raft's own
	// StopElectionTimer uses.
	stopCh   chan struct{}
	stopOnce sync.Once
}

// kvSnapshot is everything a KVServer needs to fully reconstruct its
// state without replaying a single log entry: the store itself AND the
// dedup table, serialized together so they're always restored as of
// the exact same point — restoring one without the other would let a
// request that landed right at the snapshot boundary either double-
// apply or get incorrectly treated as already-seen. Passed to Raft as
// the opaque data argument to Snapshot/delivered back via ReadSnapshot;
// Raft itself never looks inside it.
type kvSnapshot struct {
	Store          map[string]string
	DuplicateTable map[int64]int64
}

// NewKVServer wraps rf and starts the apply loop that keeps this node's
// store in sync with its Raft log. The caller is still responsible for
// starting rf's own background loops (RunElectionTimer, RunHeartbeats,
// RunApplyLoop) — NewKVServer only starts its own consumers of
// rf.ApplyCh and rf.State(), which those loops feed. Call Stop when
// done with this KVServer to shut those consumers down.
//
// maxRaftState is the size threshold (in bytes of rf's persisted state)
// that triggers a snapshot — see the maxRaftState field doc. Pass -1 to
// disable snapshotting.
//
// If rf already has a snapshot persisted (this node is restarting, not
// booting fresh — see restoreSnapshot), NewKVServer restores store and
// duplicateTable from it before starting applyLoop. This is NOT
// optional once snapshotting is in play: Snapshot has already discarded
// every log entry through the snapshot's index, so those entries can
// never arrive on ApplyCh again — the snapshot is the only remaining
// record of what they did.
func NewKVServer(rf *raft.Raft, maxRaftState int) *KVServer {
	kv := &KVServer{
		rf:             rf,
		store:          make(map[string]string),
		notifyChans:    make(map[int]chan Op),
		duplicateTable: make(map[int64]int64),
		maxRaftState:   maxRaftState,
		stopCh:         make(chan struct{}),
	}
	if data := rf.ReadSnapshot(); len(data) > 0 {
		kv.restoreSnapshot(data)
	}
	go kv.applyLoop()
	go kv.noopLoop()
	return kv
}

// Stop terminates this KVServer's background goroutines (applyLoop,
// noopLoop). Safe to call more than once. Does not touch the underlying
// *raft.Raft — stop that separately via StopElectionTimer.
func (kv *KVServer) Stop() {
	kv.stopOnce.Do(func() {
		close(kv.stopCh)
	})
}

// restoreSnapshot decodes data (as produced by snapshotLocked) and
// adopts it as this KVServer's entire state — wholesale replacing
// whatever store/duplicateTable held before, not merging into it.
// Called from two places: NewKVServer, before applyLoop starts (no
// lock needed there — nothing else can be touching kv.store/
// duplicateTable that early), and applyLoop itself (Day 7), when this
// node's underlying Raft node just installed a snapshot from a leader
// (msg.SnapshotValid) — that call happens under kv.mu, since real
// concurrent Get/PutAppend traffic can be in flight by then.
func (kv *KVServer) restoreSnapshot(data []byte) {
	var snap kvSnapshot
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&snap); err != nil {
		panic(fmt.Sprintf("kvstore: failed to decode persisted snapshot: %v", err))
	}
	kv.store = snap.Store
	kv.duplicateTable = snap.DuplicateTable
}

// snapshotLocked serializes the current store+duplicateTable and hands
// the result to Raft along with index — the log index this state
// reflects, i.e. every entry through index has already been applied —
// so Raft can discard its own log through that point. Caller must hold
// kv.mu, since it reads store/duplicateTable directly.
func (kv *KVServer) snapshotLocked(index int) {
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(kvSnapshot{Store: kv.store, DuplicateTable: kv.duplicateTable}); err != nil {
		panic(fmt.Sprintf("kvstore: failed to encode snapshot: %v", err))
	}
	if err := kv.rf.Snapshot(index, buf.Bytes()); err != nil {
		panic(fmt.Sprintf("kvstore: failed to snapshot through index %d: %v", index, err))
	}
}

// applyLoop consumes rf.ApplyCh and applies each committed Put/Append to
// the local store, in the exact order Raft committed them — except a
// duplicate (op.SeqNum <= the highest SeqNum already applied for
// op.ClientID) is recognized and its effect skipped, so a retried
// request never double-applies. Either way, the index's waiter (if any)
// is still notified: the RETRY's own RPC call still needs its own reply,
// even though the mutation it asked for already happened via an earlier
// attempt.
//
// Since Day 7, a message can also be a just-installed snapshot
// (msg.SnapshotValid) rather than a regular committed entry — this
// node's underlying Raft node fell far enough behind that the leader
// sent its whole compacted state instead of individual entries. That
// case is handled first and separately: it replaces store/
// duplicateTable wholesale rather than applying one Op. Nothing extra
// is needed for any notifyChans this node might still be holding for
// now-compacted indices — a stale wait there just never fires, which
// PutAppend's own leaderCheck ticker already handles independently
// (Day 4), the same way it handles any other loss of leadership.
func (kv *KVServer) applyLoop() {
	for {
		var msg raft.ApplyMsg
		select {
		case <-kv.stopCh:
			return
		case msg = <-kv.rf.ApplyCh:
		}

		if msg.SnapshotValid {
			kv.mu.Lock()
			kv.restoreSnapshot(msg.Snapshot)
			kv.mu.Unlock()
			continue
		}

		op, ok := msg.Command.(Op)
		if !ok {
			// This server only ever proposes Op values — a non-Op
			// command reaching here shouldn't be possible. Panicking
			// surfaces a real bug immediately instead of silently
			// corrupting the store.
			panic(fmt.Sprintf("kvstore: applyLoop received a non-Op command at index %d: %#v", msg.Index, msg.Command))
		}

		kv.mu.Lock()
		if op.Type == "Noop" {
			// Handled before the dedup guard, not inside it: a no-op has
			// ClientID 0 / SeqNum 0, and 0 > duplicateTable[0] (also 0) is
			// false, so the guard below would skip it entirely — which is
			// exactly why the "Noop" case in its switch is unreachable.
			kv.noopAppliedTerm = msg.Term
		}
		if op.SeqNum > kv.duplicateTable[op.ClientID] {
			switch op.Type {
			case "Put":
				kv.store[op.Key] = op.Value
			case "Append":
				kv.store[op.Key] += op.Value
			case "Noop":
				// Intentionally does nothing to the store — see
				// noopLoop's doc comment for why this op exists at all.
			default:
				panic(fmt.Sprintf("kvstore: applyLoop received an unknown Op type %q at index %d", op.Type, msg.Index))
			}
			kv.duplicateTable[op.ClientID] = op.SeqNum
		}
		if ch, waiting := kv.notifyChans[msg.Index]; waiting {
			delete(kv.notifyChans, msg.Index)
			ch <- op
		}
		// Checked after every applied entry, not on a separate timer:
		// RaftStateSize only grows one entry at a time, so there's no
		// way to overshoot a threshold between checks, and applyLoop
		// already holds kv.mu with a consistent, just-applied view of
		// store/duplicateTable right here — the exact state
		// snapshotLocked needs to serialize.
		if kv.maxRaftState != -1 && kv.rf.RaftStateSize() >= kv.maxRaftState {
			kv.snapshotLocked(msg.Index)
		}
		kv.mu.Unlock()
	}
}

// noopLoop watches for this node becoming leader and, once per new term
// it observes itself leading, Proposes a no-op entry. This is the
// standard fix the Raft paper itself names (§8): per Day 9's Figure 8
// safety rule, a freshly-elected leader can't mark ANY older-term entry
// committed until something in its OWN term reaches a majority — so
// immediately after an election, this node's own view of "what's
// committed" can lag behind what a majority of the cluster actually
// holds, even though the up-to-date voting rule guarantees this
// leader's LOG already has every one of those entries. Left alone, that
// window means Get can serve a stale or missing read for a key that is,
// from the cluster's perspective, already durable — exactly the failure
// TestConcurrentClientsWithFaultInjection caught. Proposing a no-op the
// instant this node becomes leader is what closes that window as fast
// as possible: once the no-op itself commits, Figure 8 transitively
// re-confirms everything before it.
//
// Every no-op uses ClientID 0, which no real Clerk will ever collide
// with (Clerk.clientID is drawn from crypto/rand, so a collision with
// the single reserved sentinel value is astronomically unlikely) — and
// since a no-op never mutates the store, whether the dedup check
// happens to treat any particular no-op as "already seen" makes no
// observable difference either way.
func (kv *KVServer) noopLoop() {
	ticker := time.NewTicker(leaderCheckInterval)
	defer ticker.Stop()

	lastNoopTerm := -1
	for {
		select {
		case <-kv.stopCh:
			return
		case <-ticker.C:
			if kv.rf.State() != raft.Leader {
				continue
			}
			term := kv.rf.Term()
			if term == lastNoopTerm {
				continue
			}
			if _, _, isLeader := kv.rf.Propose(Op{Type: "Noop"}); isLeader {
				lastNoopTerm = term
			}
		}
	}
}

// get returns the current local value for key — a DIRECT read of this
// node's own in-memory map, not routed through Raft or gated on
// leadership. Unexported: Get (below) is the real client-facing
// operation now; this stays only as the primitive it and PutAppend's
// state access build on.
func (kv *KVServer) get(key string) (value string, ok bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok = kv.store[key]
	return value, ok
}

// Get is the client-facing RPC handler for a read. It answers only if
// this node is Leader AND has applied a no-op from its own current term
// (noopAppliedTerm) — Raft §8's read rule. noopLoop proposes that no-op
// the moment a node wins an election, but proposing it isn't enough:
// until it has actually committed and applied, entries the PREVIOUS
// leader had already committed (and acknowledged to clients) may not be
// in this node's store yet. Stage 3's load test found exactly that: a
// client's Append is acknowledged by one leader, the client's next Get
// lands on a just-elected successor, and it reads a value missing the
// last acknowledged write. Until the no-op applies, Get reports
// ErrWrongLeader, which every client already retries.
//
// Get is still NOT fully linearizable: a leader that's been silently
// partitioned away doesn't know it's been superseded, and would keep
// answering from its own (now stale) store. Closing THAT gap (routing
// reads through the log, or a read-index / leader-lease scheme) remains
// future work.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) error {
	if kv.rf.State() != raft.Leader {
		reply.Err = ErrWrongLeader
		return nil
	}
	// Term is read BEFORE noopAppliedTerm on purpose: if leadership
	// changes between the two reads, the older term can only be <= the
	// applied one, and a stale match here is caught by the State check
	// having just passed for a node still leading that term.
	term := kv.rf.Term()
	kv.mu.Lock()
	ready := kv.noopAppliedTerm == term
	kv.mu.Unlock()
	if !ready {
		reply.Err = ErrWrongLeader
		return nil
	}
	value, ok := kv.get(args.Key)
	if !ok {
		reply.Err = ErrNoKey
		return nil
	}
	reply.Value = value
	reply.Err = OK
	return nil
}

// PutAppend is the client-facing RPC handler for a write. It Proposes
// the corresponding Op to Raft and waits for THAT SPECIFIC log index to
// commit — not just "wait for commitIndex to reach some number" —
// because a Proposed-but-uncommitted entry can be overwritten by a
// later leader's entry at the same index if this node loses leadership
// before a majority ever confirms it. There are two distinct ways this
// handler learns its proposal is doomed, and it needs both:
//
//  1. A DIFFERENT Op ends up committed at the same index (the notify
//     channel delivers it) — this proposal was superseded by whichever
//     leader's entry actually won that slot.
//  2. This node stops being leader of the term it Proposed in, before
//     anything else ever lands at that index (Day 4). Nothing then
//     guarantees the notify channel ever fires again — the index might
//     never be touched by a future leader at all — so waiting for case 1
//     or the full commitTimeout would be needlessly slow when the answer
//     is already knowable. leaderCheckInterval polls for exactly this.
//
// Either way the client is told the same thing: retry, almost certainly
// against a new leader, rather than getting a false OK for an entry that
// got silently discarded.
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	op := Op{Type: args.Op, Key: args.Key, Value: args.Value, ClientID: args.ClientID, SeqNum: args.SeqNum}

	index, term, isLeader := kv.rf.Propose(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return nil
	}

	kv.mu.Lock()
	ch := make(chan Op, 1)
	kv.notifyChans[index] = ch
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.notifyChans, index)
		kv.mu.Unlock()
	}()

	leaderCheck := time.NewTicker(leaderCheckInterval)
	defer leaderCheck.Stop()
	deadline := time.After(commitTimeout)

	for {
		select {
		case applied := <-ch:
			if applied != op {
				reply.Err = ErrWrongLeader
				return nil
			}
			reply.Err = OK
			return nil
		case <-deadline:
			reply.Err = ErrTimeout
			return nil
		case <-leaderCheck.C:
			// Checking BOTH the term and State() != Leader is belt and
			// suspenders around the same underlying fact: as long as this
			// node has remained Leader continuously since Propose, in
			// the SAME term, nothing else could have written to this
			// index without its own consent — the term changing (or the
			// state no longer being Leader) is what actually signals a
			// step-down happened.
			if kv.rf.Term() != term || kv.rf.State() != raft.Leader {
				reply.Err = ErrWrongLeader
				return nil
			}
		}
	}
}
