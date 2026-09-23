package pool

import (
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"testing"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
	kvstore "github.com/aksharraikanti/distsys-lab/02-kv-store"
)

// crashableEndpoint is a KVServer's real TCP face (what a PooledClient
// actually connects to), with a working crash()/restart(). ServeKVServer
// can't do this: net/rpc keeps serving already-accepted connections
// after their listener closes (Day 4 found this the hard way), so a real
// crash — every established connection severed, new dials refused —
// needs the server side to track what it accepted. That tracking lives
// here, in test code, rather than growing ServeKVServer a shutdown API
// nothing outside this test needs.
type crashableEndpoint struct {
	kv *kvstore.KVServer

	mu    sync.Mutex
	addr  string
	l     net.Listener
	conns []net.Conn
}

func (e *crashableEndpoint) start() error {
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

// crash refuses new connections and severs every established one — what
// a client of a really-crashed process observes.
func (e *crashableEndpoint) crash() {
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

// faultyTCPCluster is tcpKVCluster with the two fault surfaces this
// test needs: the Raft transport (leader crash / partition, as in
// Stage 2 Days 5 and 8) AND each node's client-facing TCP endpoint
// (crashableEndpoint) — the surface Stage 3 added, which no earlier
// stage's faults could reach.
func faultyTCPCluster(t *testing.T, n int) (nodes map[int]*raft.Raft, eps []*crashableEndpoint, transport *raft.FakeTransport, addrs []string, cleanup func()) {
	t.Helper()
	transport = raft.NewFakeTransport()
	ids := make([]int, n)
	for i := range ids {
		ids[i] = i
	}
	nodes = make(map[int]*raft.Raft, n)
	kvs := make([]*kvstore.KVServer, n)
	eps = make([]*crashableEndpoint, n)
	addrs = make([]string, n)
	for _, id := range ids {
		rf := raft.NewRaft(id, otherPeers(ids, id), transport)
		nodes[id] = rf
		transport.Register(id, rf)
		kvs[id] = kvstore.NewKVServer(rf, -1)
		eps[id] = &crashableEndpoint{kv: kvs[id], addr: "127.0.0.1:0"}
		if err := eps[id].start(); err != nil {
			t.Fatalf("endpoint %d start: %v", id, err)
		}
		addrs[id] = eps[id].addr
	}
	for _, rf := range nodes {
		go rf.RunElectionTimer()
		go rf.RunHeartbeats()
		go rf.RunApplyLoop()
	}
	cleanup = func() {
		for _, rf := range nodes {
			rf.StopElectionTimer()
		}
		for _, kv := range kvs {
			kv.Stop()
		}
		for _, e := range eps {
			e.crash()
		}
	}
	return nodes, eps, transport, addrs, cleanup
}

func waitForSingleLeader(t *testing.T, nodes map[int]*raft.Raft, timeout time.Duration) {
	t.Helper()
	waitFor(t, timeout, func() bool {
		leaders := 0
		for _, rf := range nodes {
			if rf.State() == raft.Leader {
				leaders++
			}
		}
		return leaders == 1
	})
}

// TestPoolLoadThroughRealFaults is Stage 3's full-integration test, the
// counterpart to Stage 2's Day 8: concurrent PooledClients hammering a
// real-TCP cluster while three different faults fire in rotation —
// a node's TCP endpoint crashing and restarting (severing every pooled
// connection to it, then refusing dials until it's back), the Raft
// leader being cut off, and a majority/minority network partition.
//
// Three properties, matching TASKS.md's own list:
//   - Correctness survives: every client's private-key Append history
//     reads back exactly (Stage 2's invariant, now through the pool).
//   - No deadlock under backpressure: the test finishing at all — with
//     pools deliberately smaller than the caller count — is the proof.
//   - No connection leak, and the pool heals: once everything settles,
//     every pool's live count is within its bounds and every connection
//     is back in its free list (none stuck "checked out"), and a fresh
//     client can read everything back.
func TestPoolLoadThroughRealFaults(t *testing.T) {
	const numNodes = 3
	const numClients = 6
	const opsPerClient = 400
	const minSize, maxSize = 1, 2 // fewer connections than callers: forces real backpressure

	nodes, eps, transport, addrs, cleanup := faultyTCPCluster(t, numNodes)
	defer cleanup()
	waitForSingleLeader(t, nodes, 20*raft.ElectionTimeoutMax)

	// Constructed BEFORE any fault starts: NewPooledClient dials MinSize
	// connections up front and (correctly) fails if an address is down.
	clients := make([]*PooledClient, numClients)
	for i := range clients {
		c, err := NewPooledClient(addrs, minSize, maxSize)
		if err != nil {
			t.Fatalf("NewPooledClient %d: %v", i, err)
		}
		clients[i] = c
	}

	faultStop := make(chan struct{})
	var faultWg sync.WaitGroup
	faultWg.Add(1)
	faultRounds := 0 // written only by the injector goroutine; read after faultWg.Wait
	go func() {
		defer faultWg.Done()
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		// The first fault fires immediately, not after a tick: an earlier
		// draft of this test waited a full interval first and the whole
		// workload finished inside it, so it "passed" having tested no
		// faults at all.
		for round := 0; ; round++ {
			select {
			case <-faultStop:
				return
			case <-time.After(2 * raft.ElectionTimeoutMax):
			}
			faultRounds++
			switch round % 3 {
			case 0: // a node's TCP endpoint crashes, then restarts on the same address
				e := eps[rng.Intn(numNodes)]
				e.crash()
				time.Sleep(3 * raft.ElectionTimeoutMax)
				if err := e.start(); err != nil {
					t.Errorf("endpoint restart: %v", err)
				}
			case 1: // the Raft leader is cut off, then rejoins
				for id, rf := range nodes {
					if rf.State() == raft.Leader {
						transport.Unregister(id)
						time.Sleep(2 * raft.ElectionTimeoutMax)
						transport.Register(id, rf)
						break
					}
				}
			case 2: // majority/minority partition, then heal
				transport.Partition([]int{0, 1}, []int{2})
				time.Sleep(3 * raft.ElectionTimeoutMax)
				transport.Heal()
			}
		}
	}()

	var wg sync.WaitGroup
	errs := make(chan error, numClients)
	for i, c := range clients {
		wg.Add(1)
		go func(clientNum int, c *PooledClient) {
			defer wg.Done()
			key := fmt.Sprintf("client-%d", clientNum)
			var expected string
			for j := 0; j < opsPerClient; j++ {
				frag := fmt.Sprintf("[%d]", j)
				c.Append(key, frag)
				expected += frag
				if got := c.Get(key); got != expected {
					errs <- fmt.Errorf("client %d: after %d appends, Get = %q, want %q", clientNum, j+1, got, expected)
					return
				}
			}
		}(i, c)
	}
	wg.Wait()

	close(faultStop)
	faultWg.Wait()
	if faultRounds < 3 {
		t.Errorf("only %d fault rounds ran, want at least 3 (one of each kind) — the workload finished too fast to test anything", faultRounds)
	}
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	// Every fault has ended (faultWg.Wait above returns only after the
	// injector finished any in-flight crash/restart), so the cluster is
	// fully reachable again. Check the pools themselves: nothing leaked,
	// everything within bounds.
	for i, c := range clients {
		for addr, p := range c.pooled.pools {
			p.mu.Lock()
			count := p.count
			p.mu.Unlock()
			if count < 0 || count > maxSize {
				t.Errorf("client %d pool %s: count = %d, want within [0, %d]", i, addr, count, maxSize)
			}
			if free := len(p.free); free > count {
				t.Errorf("client %d pool %s: %d free connections but only %d counted", i, addr, free, count)
			}
		}
	}

	// Pool recovery, checked at the pool level (not just "the client
	// eventually got there via retries"): with the whole cluster back up,
	// every pool must be able to serve a real call again — the endpoints
	// that were crashed had their connections evicted and replaced, not
	// left as dead entries in the free list.
	for _, c := range clients {
		for _, p := range c.pooled.pools {
			var reply kvstore.GetReply
			waitFor(t, 40*raft.ElectionTimeoutMax, func() bool {
				return p.Call("KVServer.Get", &kvstore.GetArgs{Key: "probe"}, &reply) == nil
			})
		}
	}

	// And a brand-new client reads back everything every old client wrote.
	fresh, err := NewPooledClient(addrs, minSize, maxSize)
	if err != nil {
		t.Fatalf("fresh NewPooledClient: %v", err)
	}
	for i := 0; i < numClients; i++ {
		key := fmt.Sprintf("client-%d", i)
		var want string
		for j := 0; j < opsPerClient; j++ {
			want += fmt.Sprintf("[%d]", j)
		}
		waitFor(t, 40*raft.ElectionTimeoutMax, func() bool { return fresh.Get(key) == want })
	}

	// Close everything: Close's own precondition (no calls in flight)
	// holds, and it must not hang on any background redial/evictor
	// goroutine.
	fresh.Close()
	for _, c := range clients {
		c.Close()
	}
}
