// Package kvtest is shared test support for building Raft-backed KV clusters
// whose client-facing TCP endpoints can be crashed and restarted. Stage 5's
// shard groups are each one such cluster.
//
// It exists because the same ~100 lines (a listener that tracks accepted
// connections so a "crash" can sever them, plus the wiring of Raft nodes,
// KVServers and endpoints) had been copied into Stage 3's and Stage 4's tests,
// and Stage 5 would have been the third copy. net/rpc keeps serving already-
// accepted connections after their listener closes, so ServeKVServer alone
// cannot simulate a crash — hence the tracking here. Being a non-test package
// under internal/, any stage in the module can import it. Stage 3's and 4's
// existing copies have not been migrated onto it.
package kvtest

import (
	"net"
	"net/rpc"
	"sync"
	"testing"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
	kvstore "github.com/aksharraikanti/distsys-lab/02-kv-store"
)

// Endpoint is one KVServer's real TCP face with a working Crash/Start: Crash
// refuses new connections AND severs every established one, which is what a
// client of a really-crashed process observes.
type Endpoint struct {
	kv *kvstore.KVServer

	mu    sync.Mutex
	addr  string
	l     net.Listener
	conns []net.Conn
}

// Addr returns the address the endpoint listens on (stable across restarts).
func (e *Endpoint) Addr() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.addr
}

// Start begins listening (on the same address as before, after the first
// start) and serving.
func (e *Endpoint) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	l, err := net.Listen("tcp", e.addr)
	if err != nil {
		return err
	}
	e.l = l
	e.addr = l.Addr().String()
	e.conns = nil

	server := rpc.NewServer()
	if err := server.RegisterName("KVServer", e.kv); err != nil {
		l.Close()
		return err
	}
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			e.mu.Lock()
			if e.l != l { // crashed between Accept returning and here
				e.mu.Unlock()
				c.Close()
				return
			}
			e.conns = append(e.conns, c)
			e.mu.Unlock()
			go server.ServeConn(c)
		}
	}()
	return nil
}

func (e *Endpoint) running() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.l != nil
}

// Crash refuses new connections and severs every established one.
func (e *Endpoint) Crash() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.l == nil {
		return
	}
	e.l.Close()
	e.l = nil
	for _, c := range e.conns {
		c.Close()
	}
	e.conns = nil
}

// Cluster is an n-node Raft cluster of KVServers, each exposed over its own
// crashable TCP endpoint. Raft traffic between nodes goes over a FakeTransport
// (as in every earlier stage's tests); only the CLIENT-facing side is real TCP.
type Cluster struct {
	Nodes     map[int]*raft.Raft
	KVs       []*kvstore.KVServer
	Endpoints []*Endpoint
	Transport *raft.FakeTransport
	Addrs     []string
}

// NewCluster starts an n-node cluster and waits until it is actually serving
// reads (a fresh leader refuses Get until its own-term no-op applies), so
// callers never race a cold cluster. maxRaftState is passed to each KVServer
// (-1 disables snapshotting).
func NewCluster(tb testing.TB, n, maxRaftState int) *Cluster {
	tb.Helper()
	transport := raft.NewFakeTransport()
	ids := make([]int, n)
	for i := range ids {
		ids[i] = i
	}
	c := &Cluster{
		Nodes:     make(map[int]*raft.Raft, n),
		KVs:       make([]*kvstore.KVServer, n),
		Endpoints: make([]*Endpoint, n),
		Transport: transport,
		Addrs:     make([]string, n),
	}
	for _, id := range ids {
		var peers []int
		for _, o := range ids {
			if o != id {
				peers = append(peers, o)
			}
		}
		rf := raft.NewRaft(id, peers, transport)
		c.Nodes[id] = rf
		transport.Register(id, rf)
		c.KVs[id] = kvstore.NewKVServer(rf, maxRaftState)
		c.Endpoints[id] = &Endpoint{kv: c.KVs[id], addr: "127.0.0.1:0"}
		if err := c.Endpoints[id].Start(); err != nil {
			tb.Fatalf("kvtest: endpoint %d: %v", id, err)
		}
		c.Addrs[id] = c.Endpoints[id].Addr()
	}
	for _, rf := range c.Nodes {
		go rf.RunElectionTimer()
		go rf.RunHeartbeats()
		go rf.RunApplyLoop()
	}

	deadline := time.Now().Add(5 * time.Second)
	for !c.serving() {
		if time.Now().After(deadline) {
			c.Close()
			tb.Fatal("kvtest: cluster never started serving reads")
		}
		time.Sleep(time.Millisecond)
	}
	return c
}

// serving reports whether some node currently answers a Get as leader.
func (c *Cluster) serving() bool {
	for _, kv := range c.KVs {
		var reply kvstore.GetReply
		if err := kv.Get(&kvstore.GetArgs{Key: "kvtest-ready"}, &reply); err == nil && reply.Err != kvstore.ErrWrongLeader {
			return true
		}
	}
	return false
}

// CrashAll takes every endpoint down: a total outage of the client-facing
// side (the Raft nodes keep running, exactly like Stage 4's outage test).
func (c *Cluster) CrashAll() {
	for _, e := range c.Endpoints {
		e.Crash()
	}
}

// RestartAll brings every crashed endpoint back on its original address.
func (c *Cluster) RestartAll() error {
	for _, e := range c.Endpoints {
		if e.running() {
			continue
		}
		if err := e.Start(); err != nil {
			return err
		}
	}
	return nil
}

// Close stops everything. Safe to call once.
func (c *Cluster) Close() {
	for _, rf := range c.Nodes {
		rf.StopElectionTimer()
	}
	for _, kv := range c.KVs {
		kv.Stop()
	}
	for _, e := range c.Endpoints {
		e.Crash()
	}
}
