package raft

import "testing"

// TestFakeTransportThreeNodes wires 3 nodes together over a FakeTransport
// and confirms every node can reach every other node — the Day 1 bar:
// "get 3 in-process mock nodes talking over it."
//
// This only checks RPC connectivity (calls succeed, replies come back),
// not voting/heartbeat semantics — Day 4 gave RequestVote real logic and
// Day 5 gave AppendEntries real logic; election_test.go and
// heartbeat_test.go are where that behavior is exhaustively tested. Using
// Term: -1 for both calls here (below any node's starting currentTerm of
// 0) keeps this a deterministic connectivity check without re-testing
// either day's rules.
func TestFakeTransportThreeNodes(t *testing.T) {
	transport := NewFakeTransport()
	ids := []int{0, 1, 2}
	nodes := make(map[int]*Raft, len(ids))

	for _, id := range ids {
		peers := otherPeers(ids, id)
		node := NewRaft(id, peers, transport)
		nodes[id] = node
		transport.Register(id, node)
	}

	for _, from := range ids {
		for _, to := range ids {
			if from == to {
				continue
			}
			var reply RequestVoteReply
			args := &RequestVoteArgs{Term: -1, CandidateID: from}
			if err := nodes[from].transport.CallRequestVote(to, args, &reply); err != nil {
				t.Fatalf("node %d -> node %d RequestVote failed: %v", from, to, err)
			}
			if reply.VoteGranted {
				t.Fatalf("node %d granted a vote for a stale (negative) term; expected false", to)
			}

			var aeReply AppendEntriesReply
			aeArgs := &AppendEntriesArgs{Term: -1, LeaderID: from}
			if err := nodes[from].transport.CallAppendEntries(to, aeArgs, &aeReply); err != nil {
				t.Fatalf("node %d -> node %d AppendEntries failed: %v", from, to, err)
			}
			if aeReply.Success {
				t.Fatalf("node %d reported AppendEntries success for a stale (negative) term; expected false", to)
			}
		}
	}
}

// TestNetTransportThreeNodes proves the same round trip over a real TCP
// loopback via net/rpc — Day 1's "real net/rpc for normal runs" half of
// the transport interface, not just the fake one tests will use later.
//
// Each node needs its peers' addresses before it can dial them, but an
// address is only known once its server is listening — so nodes are
// constructed first with no transport, started on OS-assigned ports to
// learn every address, then wired to a shared NetTransport. Direct field
// assignment (node.transport = ...) is available here because this file
// is an internal test (package raft, not raft_test).
func TestNetTransportThreeNodes(t *testing.T) {
	ids := []int{0, 1, 2}
	nodes := make(map[int]*Raft, len(ids))
	for _, id := range ids {
		nodes[id] = NewRaft(id, otherPeers(ids, id), nil)
	}

	addrs := make(map[int]string, len(ids))
	for _, id := range ids {
		l, err := ServeNetTransport("127.0.0.1:0", nodes[id])
		if err != nil {
			t.Fatalf("failed to serve node %d: %v", id, err)
		}
		defer l.Close()
		addrs[id] = l.Addr().String()
	}

	transport := NewNetTransport(addrs)
	for _, id := range ids {
		nodes[id].transport = transport
	}

	var reply RequestVoteReply
	// Term: -1 (below any node's starting currentTerm of 0) keeps this a
	// pure connectivity check — see TestFakeTransportThreeNodes for why.
	if err := transport.CallRequestVote(1, &RequestVoteArgs{Term: -1, CandidateID: 0}, &reply); err != nil {
		t.Fatalf("node 0 -> node 1 RequestVote over net/rpc failed: %v", err)
	}
	if reply.VoteGranted {
		t.Fatalf("node 1 granted a vote for a stale (negative) term; expected false")
	}
}

func otherPeers(ids []int, self int) []int {
	peers := make([]int, 0, len(ids)-1)
	for _, id := range ids {
		if id != self {
			peers = append(peers, id)
		}
	}
	return peers
}
