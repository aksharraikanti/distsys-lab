package pool

import (
	"fmt"
	"net/rpc"
)

// Pool is a fixed-size set of already-dialed *rpc.Client connections
// to ONE server address, checked out before a call and checked back in
// after — a request reuses an existing connection instead of paying a
// fresh TCP handshake every time (see NaiveClient/
// BenchmarkNaiveClientPutAppend for the cold-start cost this exists to
// avoid).
//
// "Fixed-size" is deliberately literal for Day 2: exactly size
// connections are dialed once, at construction, and the set never
// grows or shrinks afterward. A checkout beyond that many concurrent
// callers blocks until another caller checks one back in — that's free
// behavior from using a buffered channel as the free list, not yet a
// deliberate policy. Day 3 makes "what happens under exhaustion" an
// explicit choice (block-with-timeout, an overflow queue, or reject).
// Day 4 adds detecting and replacing a connection that's gone bad. Day
// 5 adds idle eviction and resizing. None of that exists yet — this is
// the simplest version that's still correct.
type Pool struct {
	addr string
	free chan *rpc.Client
}

// NewPool dials size connections to addr up front and returns a Pool
// ready to hand them out. If any dial fails, every connection already
// established is closed before returning the error — a partially
// initialized pool would be a silent resource leak, not a smaller pool.
func NewPool(addr string, size int) (*Pool, error) {
	conns := make([]*rpc.Client, 0, size)
	for i := 0; i < size; i++ {
		c, err := rpc.Dial("tcp", addr)
		if err != nil {
			for _, c := range conns {
				c.Close()
			}
			return nil, fmt.Errorf("pool: dialing connection %d/%d to %s: %w", i+1, size, addr, err)
		}
		conns = append(conns, c)
	}
	free := make(chan *rpc.Client, size)
	for _, c := range conns {
		free <- c
	}
	return &Pool{addr: addr, free: free}, nil
}

// Call checks out a connection, makes the RPC, and checks the
// connection back in — even on error. A naive fixed-size pool doesn't
// yet distinguish a transient RPC-level error (the KV server replying
// ErrWrongLeader, say — a perfectly healthy connection, just to the
// wrong node) from a genuinely broken connection; that distinction,
// and evicting/replacing a connection that's actually dead, is Day 4's
// job. For now every checked-out connection always comes back to the
// free list.
func (p *Pool) Call(method string, args, reply interface{}) error {
	c := <-p.free
	defer func() { p.free <- c }()
	return c.Call(method, args, reply)
}

// Close closes every connection currently in the pool. Callers must
// not still have a connection checked out via Call when Close runs —
// the same "no more calls in flight" precondition every Stop/Close in
// this codebase already relies on its caller to uphold (see
// 02-kv-store's KVServer.Stop, 01-raft's StopElectionTimer).
func (p *Pool) Close() error {
	close(p.free)
	var firstErr error
	for c := range p.free {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// pooledCaller routes each call through the Pool for that specific
// address — one Pool per address, size connections pre-dialed to EVERY
// address up front. Most of those connections sit idle at any given
// moment, since only the current leader actually answers a call with
// OK, but they're ready the instant a leadership change makes a
// different address the one that matters, rather than paying a fresh
// dial right when it's needed most.
type pooledCaller struct {
	pools map[string]*Pool
}

func newPooledCaller(addrs []string, size int) (*pooledCaller, error) {
	pools := make(map[string]*Pool, len(addrs))
	for _, addr := range addrs {
		p, err := NewPool(addr, size)
		if err != nil {
			for _, p := range pools {
				p.Close()
			}
			return nil, err
		}
		pools[addr] = p
	}
	return &pooledCaller{pools: pools}, nil
}

func (pc *pooledCaller) call(addr, method string, args, reply interface{}) error {
	return pc.pools[addr].Call(method, args, reply)
}

func (pc *pooledCaller) close() error {
	var firstErr error
	for _, p := range pc.pools {
		if err := p.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// PooledClient is Day 2's whole point: the SAME retry/dedup logic
// NaiveClient uses (see client.go's shared client type), backed by a
// fixed-size Pool per server address instead of a fresh dial on every
// call.
type PooledClient struct {
	*client
	pooled *pooledCaller
}

// NewPooledClient returns a PooledClient addressing any of addrs, with
// poolSize connections pre-dialed to EACH address. Call Close when
// done to release every underlying connection.
func NewPooledClient(addrs []string, poolSize int) (*PooledClient, error) {
	pc, err := newPooledCaller(addrs, poolSize)
	if err != nil {
		return nil, err
	}
	return &PooledClient{client: newClient(addrs, pc), pooled: pc}, nil
}

// Close releases every connection this client has pooled.
func (c *PooledClient) Close() error {
	return c.pooled.close()
}
