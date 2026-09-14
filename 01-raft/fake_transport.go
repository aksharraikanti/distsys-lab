package raft

import (
	"fmt"
	"sync"
)

// FakeTransport delivers RPCs in-process by calling straight into a
// registered peer's RPCHandler, instead of going over a real socket. It
// exists so tests can run many nodes in one process, quickly and
// deterministically, and — since Day 12 — so tests can inject the two
// network faults a real cluster actually has to survive: a node going
// fully unreachable (Unregister, simulating a crash) and the network
// splitting into groups that can't reach each other (Partition,
// simulating a network partition). Neither fault touches the RPCHandlers
// themselves; a "crashed" node's Raft instance keeps running exactly as
// before, it's just unreachable — which is precisely what a real crash
// looks like from every OTHER node's point of view.
type FakeTransport struct {
	mu    sync.Mutex
	peers map[int]RPCHandler

	// partition maps a peer id to the id of the group it currently
	// belongs to. nil means "no partition configured" — the default,
	// fully-connected state. Two peers can reach each other only if
	// partition is nil or they share the same group id.
	partition map[int]int
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

// Unregister removes peer id from the transport — any Call* directed at
// it now fails, exactly as a real RPC to a crashed process would. This
// is deliberately different from just stopping a node's own background
// loops (RunElectionTimer/RunHeartbeats/RunApplyLoop): a node that only
// stopped INITIATING calls would still correctly ANSWER any RPC sent to
// it, which is not what a crashed node does. Re-`Register` the same (or
// a freshly restarted) instance under the same id to simulate recovery.
func (t *FakeTransport) Unregister(id int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.peers, id)
}

// Partition splits the network into disjoint groups: peers in different
// groups can no longer reach each other in either direction, simulating
// a network partition. Peers within the same group can still reach each
// other normally. Call Heal to restore full connectivity.
func (t *FakeTransport) Partition(groups ...[]int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.partition = make(map[int]int, len(t.peers))
	for groupID, group := range groups {
		for _, peer := range group {
			t.partition[peer] = groupID
		}
	}
}

// Heal restores full connectivity — every peer can reach every peer
// again, as if Partition was never called.
func (t *FakeTransport) Heal() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.partition = nil
}

// reachable reports whether from can currently reach to: always true
// with no partition configured, otherwise only if both are assigned to
// the same group. A peer with no group assignment at all (registered
// after Partition was called) can't reach, or be reached by, anyone —
// treated as its own isolated group of one, the safest default for an
// id the partition configuration doesn't know about.
func (t *FakeTransport) reachable(from, to int) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.partition == nil {
		return true
	}
	fromGroup, fromOK := t.partition[from]
	toGroup, toOK := t.partition[to]
	return fromOK && toOK && fromGroup == toGroup
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
	if !t.reachable(args.CandidateID, peer) {
		return fmt.Errorf("raft: no route from %d to %d (partitioned)", args.CandidateID, peer)
	}
	h, err := t.handlerFor(peer)
	if err != nil {
		return err
	}
	return h.RequestVote(args, reply)
}

func (t *FakeTransport) CallAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	if !t.reachable(args.LeaderID, peer) {
		return fmt.Errorf("raft: no route from %d to %d (partitioned)", args.LeaderID, peer)
	}
	h, err := t.handlerFor(peer)
	if err != nil {
		return err
	}
	return h.AppendEntries(args, reply)
}
