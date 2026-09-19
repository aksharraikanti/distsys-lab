// Package pool puts a real network boundary in front of Stage 2's
// KVServer, then builds a connection pool on top of it. See
// 03-connection-pooling/README.md for concept notes and
// 03-connection-pooling/TASKS.md for the day-by-day build plan this
// file follows.
package pool

import (
	"net"
	"net/rpc"

	kvstore "github.com/aksharraikanti/distsys-lab/02-kv-store"
)

// ServeKVServer exposes kv over real TCP via net/rpc, mirroring
// 01-raft's own NetTransport/ServeNetTransport split — real,
// over-the-wire RPC for actual runs, in-process calls (Stage 2's own
// Clerk, which every test through Stage 2 Day 8 used) for fast,
// deterministic tests. Unlike 01-raft's raftRPCService, no wrapper
// type is needed here: KVServer.Get and KVServer.PutAppend already
// have the exact shape net/rpc requires (an exported method on an
// exported type, taking a pointer-to-Args and a pointer-to-Reply,
// returning error) — the interface net_transport.go's raftRPCService
// wrapper had to bridge (raft.RPCHandler) simply doesn't exist on this
// side, since Clerk never called through an interface to begin with.
//
// Returns the listener so the caller can close it during shutdown or
// between tests, same contract as ServeNetTransport.
func ServeKVServer(addr string, kv *kvstore.KVServer) (net.Listener, error) {
	server := rpc.NewServer()
	if err := server.RegisterName("KVServer", kv); err != nil {
		return nil, err
	}
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	go server.Accept(l)
	return l, nil
}
