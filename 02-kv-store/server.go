package kvstore

import (
	"fmt"
	"sync"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
)

// commitTimeout bounds how long a client-facing RPC handler waits for
// its proposed command to actually commit before giving up and telling
// the client to retry elsewhere. Raft never guarantees a Proposed entry
// commits — this node can lose leadership before a majority confirms it
// (see PutAppend, and Day 4's closer look at that scenario) — so waiting
// forever isn't an option.
const commitTimeout = 20 * raft.ElectionTimeoutMax

// KVServer is a key-value state machine driven by a Raft node's
// committed log. As of Day 2, Get and PutAppend are real client-facing
// operations: PutAppend proposes to Raft and waits for its specific
// entry to commit before replying; Get answers directly from local
// state, gated on this node believing itself to be the leader. There is
// still no duplicate request detection (Day 3) and no handling of a
// leader that discovers, only after already replying, that it never
// actually held a majority for that term (Day 4).
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
}

// NewKVServer wraps rf and starts the apply loop that keeps this node's
// store in sync with its Raft log. The caller is still responsible for
// starting rf's own background loops (RunElectionTimer, RunHeartbeats,
// RunApplyLoop) — NewKVServer only starts its own consumer of
// rf.ApplyCh, which those loops feed.
func NewKVServer(rf *raft.Raft) *KVServer {
	kv := &KVServer{
		rf:          rf,
		store:       make(map[string]string),
		notifyChans: make(map[int]chan Op),
	}
	go kv.applyLoop()
	return kv
}

// applyLoop consumes rf.ApplyCh and applies each committed Put/Append to
// the local store, in the exact order Raft committed them, and wakes up
// any PutAppend call waiting on that specific index.
func (kv *KVServer) applyLoop() {
	for msg := range kv.rf.ApplyCh {
		op, ok := msg.Command.(Op)
		if !ok {
			// This server only ever proposes Op values — a non-Op
			// command reaching here shouldn't be possible. Panicking
			// surfaces a real bug immediately instead of silently
			// corrupting the store.
			panic(fmt.Sprintf("kvstore: applyLoop received a non-Op command at index %d: %#v", msg.Index, msg.Command))
		}

		kv.mu.Lock()
		switch op.Type {
		case "Put":
			kv.store[op.Key] = op.Value
		case "Append":
			kv.store[op.Key] += op.Value
		default:
			panic(fmt.Sprintf("kvstore: applyLoop received an unknown Op type %q at index %d", op.Type, msg.Index))
		}
		if ch, waiting := kv.notifyChans[msg.Index]; waiting {
			delete(kv.notifyChans, msg.Index)
			ch <- op
		}
		kv.mu.Unlock()
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

// Get is the client-facing RPC handler for a read. Still NOT
// linearizable: it checks that this node currently believes itself to
// be Leader, but that belief can be stale (e.g. a former leader that's
// been silently partitioned away doesn't know it's been superseded) —
// closing that gap for real (routing reads through the log, or a
// leader-lease scheme) is explicitly a later day's job, not this one's.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) error {
	if kv.rf.State() != raft.Leader {
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
// before a majority ever confirms it. If a DIFFERENT Op ends up
// committed at that index, this proposal was superseded: the client
// should retry (almost certainly against a new leader), so that case is
// reported the same way as ErrWrongLeader rather than as a false OK.
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	op := Op{Type: args.Op, Key: args.Key, Value: args.Value}

	index, _, isLeader := kv.rf.Propose(op)
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

	select {
	case applied := <-ch:
		if applied != op {
			reply.Err = ErrWrongLeader
			return nil
		}
		reply.Err = OK
	case <-time.After(commitTimeout):
		reply.Err = ErrTimeout
	}
	return nil
}
