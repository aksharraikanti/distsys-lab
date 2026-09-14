package kvstore

import (
	"fmt"
	"sync"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
)

// KVServer is a key-value state machine driven by a Raft node's
// committed log. Day 1 scope: the apply loop exists and correctly builds
// up the map from Put/Append commands, in the order Raft committed them.
// There is no client-facing Get/PutAppend RPC serving yet (Day 2), and no
// duplicate request detection (Day 3) — Propose is called directly by
// tests for now, the same way Stage 1's own early days drove Raft
// through its low-level methods before a real caller existed.
type KVServer struct {
	mu    sync.Mutex
	rf    *raft.Raft
	store map[string]string
}

// NewKVServer wraps rf and starts the apply loop that keeps this node's
// store in sync with its Raft log. The caller is still responsible for
// starting rf's own background loops (RunElectionTimer, RunHeartbeats,
// RunApplyLoop) — NewKVServer only starts its own consumer of
// rf.ApplyCh, which those loops feed.
func NewKVServer(rf *raft.Raft) *KVServer {
	kv := &KVServer{
		rf:    rf,
		store: make(map[string]string),
	}
	go kv.applyLoop()
	return kv
}

// applyLoop consumes rf.ApplyCh and applies each committed Put/Append to
// the local store, in the exact order Raft committed them. That ordering
// guarantee — not anything KVServer does itself — is what makes
// concurrent Put/Append calls from different clients converge to the
// same final value on every node in the cluster.
func (kv *KVServer) applyLoop() {
	for msg := range kv.rf.ApplyCh {
		op, ok := msg.Command.(Op)
		if !ok {
			// This server only ever proposes Op values (see Day 2's
			// client-facing handlers) — a non-Op command reaching here
			// shouldn't be possible. Panicking surfaces a real bug
			// immediately instead of silently corrupting the store.
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
		kv.mu.Unlock()
	}
}

// Get returns the current local value for key. This is a DIRECT read of
// this node's own in-memory map — it does not go through Raft, and does
// not check whether this node is even the current leader. It is
// therefore NOT linearizable yet: a partitioned-away former leader (or
// any ordinary follower) can serve a stale read this way. Making Get
// linearizable is explicitly a later day's job; Day 1's only job is
// proving the apply loop itself is correct.
func (kv *KVServer) Get(key string) (value string, ok bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok = kv.store[key]
	return value, ok
}
