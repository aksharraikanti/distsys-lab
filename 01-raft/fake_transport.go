package raft

import (
	"fmt"
	"sync"
)

// FakeTransport delivers RPCs in-process by calling straight into a
// registered peer's RPCHandler, instead of going over a real socket. It
// exists so tests can run many nodes in one process, quickly and
// deterministically. Stage 1 Day 12's fault-injection tests will extend
// this type to drop, delay, or duplicate calls on demand; for Day 1 it
// just delivers everything.
type FakeTransport struct {
	mu    sync.Mutex
	peers map[int]RPCHandler
}

// NewFakeTransport returns an empty FakeTransport. Register each node with
// Register before any Call* method can reach it.
func NewFakeTransport() *FakeTransport {
	return &FakeTransport{peers: make(map[int]RPCHandler)}
}

// Register makes peer id reachable via this transport. Every node sharing
// a FakeTransport instance can then call every other registered node.
func (t *FakeTransport) Register(id int, handler RPCHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peers[id] = handler
}

func (t *FakeTransport) handlerFor(peer int) (RPCHandler, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	h, ok := t.peers[peer]
	if !ok {
		return nil, fmt.Errorf("raft: fake transport has no peer %d registered", peer)
	}
	return h, nil
}

func (t *FakeTransport) CallRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) error {
	h, err := t.handlerFor(peer)
	if err != nil {
		return err
	}
	return h.RequestVote(args, reply)
}

func (t *FakeTransport) CallAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	h, err := t.handlerFor(peer)
	if err != nil {
		return err
	}
	return h.AppendEntries(args, reply)
}
