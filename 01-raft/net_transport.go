package raft

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

// raftRPCService is the exported type net/rpc registers on the server
// side. net/rpc requires exported methods with signature
// func(args, reply *T) error on an exported type — it can't register an
// arbitrary interface directly, so this thin wrapper forwards to whatever
// RPCHandler a node provides.
type raftRPCService struct {
	handler RPCHandler
}

func (s *raftRPCService) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	return s.handler.RequestVote(args, reply)
}

func (s *raftRPCService) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	return s.handler.AppendEntries(args, reply)
}

// ServeNetTransport starts a net/rpc server on addr that dispatches
// incoming RequestVote/AppendEntries calls to handler. It returns the
// listener so the caller can close it during shutdown or between tests.
func ServeNetTransport(addr string, handler RPCHandler) (net.Listener, error) {
	server := rpc.NewServer()
	if err := server.RegisterName("Raft", &raftRPCService{handler: handler}); err != nil {
		return nil, err
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	go server.Accept(l)
	return l, nil
}

// NetTransport is the real, over-the-network Transport implementation,
// used for normal (non-test) runs. Each peer id maps to a "host:port"
// address; connections are dialed lazily and cached.
type NetTransport struct {
	mu      sync.Mutex
	addrs   map[int]string
	clients map[int]*rpc.Client
}

// NewNetTransport builds a NetTransport from a peer id -> address map.
func NewNetTransport(addrs map[int]string) *NetTransport {
	return &NetTransport{
		addrs:   addrs,
		clients: make(map[int]*rpc.Client),
	}
}

func (t *NetTransport) clientFor(peer int) (*rpc.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if c, ok := t.clients[peer]; ok {
		return c, nil
	}
	addr, ok := t.addrs[peer]
	if !ok {
		return nil, fmt.Errorf("raft: net transport has no address for peer %d", peer)
	}
	c, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	t.clients[peer] = c
	return c, nil
}

func (t *NetTransport) CallRequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) error {
	c, err := t.clientFor(peer)
	if err != nil {
		return err
	}
	return c.Call("Raft.RequestVote", args, reply)
}

func (t *NetTransport) CallAppendEntries(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	c, err := t.clientFor(peer)
	if err != nil {
		return err
	}
	return c.Call("Raft.AppendEntries", args, reply)
}
