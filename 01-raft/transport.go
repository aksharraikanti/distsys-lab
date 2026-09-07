package raft

// RPCHandler is implemented by a Raft node to receive incoming RPCs. Both
// the real (net/rpc) and fake (in-process) transports deliver calls to a
// node through this interface, so the node's own code never knows or
// cares which transport carried the call.
type RPCHandler interface {
	RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error
	AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error
}

// Transport abstracts how a Raft node reaches its peers. NetTransport
// (net_transport.go) carries calls over real TCP via net/rpc for normal
// runs. FakeTransport (fake_transport.go) delivers calls in-process, which
// is what makes Stage 1 Day 12's fault-injection tests tractable — a fake
// transport can drop, delay, or duplicate a message deterministically
// without touching a real socket. See 01-raft/TASKS.md.
type Transport interface {
	CallRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) error
	CallAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) error
}
