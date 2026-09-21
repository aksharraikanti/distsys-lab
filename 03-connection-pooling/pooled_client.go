package pool

import (
	"errors"
	"fmt"
	"net/rpc"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
)

// ErrPoolExhausted is returned by Call when every connection in the
// pool was still checked out by some other concurrent caller for the
// entire checkoutTimeout — see Pool's own doc comment for why a bounded
// wait, not an unbounded block or an immediate rejection, is Day 3's
// deliberate choice.
var ErrPoolExhausted = errors.New("pool: no connection became available before the checkout timeout")

// Pool is a fixed-size set of already-dialed *rpc.Client connections
// to ONE server address, checked out before a call and checked back in
// after — a request reuses an existing connection instead of paying a
// fresh TCP handshake every time (see NaiveClient/
// BenchmarkNaiveClientPutAppend for the cold-start cost this exists to
// avoid).
//
// "Fixed-size" is deliberately literal: exactly size connections are
// dialed once, at construction, and the set never grows or shrinks
// afterward (Day 5 adds resizing). More concurrent callers than size is
// the NORMAL case under real load, not an edge case — checkoutTimeout
// (Day 3) is the deliberate policy for it: Call blocks until a
// connection frees up, but only up to checkoutTimeout, then returns
// ErrPoolExhausted. Two other policies were on the table and rejected:
// an unbounded overflow queue just moves the problem (unbounded memory
// growth instead of blocked goroutines) without actually bounding wait
// time; rejecting immediately treats "busy right now" the same as
// "actually broken," which would make a routine, short-lived burst fail
// requests it could easily have absorbed. A bounded wait gives a real
// burst a real chance to drain while still failing fast enough that the
// caller's own retry loop (client.go) can fall back to a different
// server rather than hang behind this one indefinitely. Day 4 adds
// detecting and replacing a connection that's gone bad, not just busy.
type Pool struct {
	addr            string
	free            chan *rpc.Client
	checkoutTimeout time.Duration
}

// NewPool dials size connections to addr up front and returns a Pool
// ready to hand them out, waiting up to checkoutTimeout for a
// connection to free up on each Call before reporting
// ErrPoolExhausted. If any dial fails, every connection already
// established is closed before returning the error — a partially
// initialized pool would be a silent resource leak, not a smaller pool.
func NewPool(addr string, size int, checkoutTimeout time.Duration) (*Pool, error) {
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
	return &Pool{addr: addr, free: free, checkoutTimeout: checkoutTimeout}, nil
}

// Call checks out a connection (waiting up to checkoutTimeout — see
// ErrPoolExhausted), makes the RPC, and checks the connection back in —
// even on error. A naive fixed-size pool doesn't yet distinguish a
// transient RPC-level error (the KV server replying ErrWrongLeader,
// say — a perfectly healthy connection, just to the wrong node) from a
// genuinely broken connection; that distinction, and evicting/
// replacing a connection that's actually dead, is Day 4's job. For now
// every checked-out connection always comes back to the free list.
func (p *Pool) Call(method string, args, reply interface{}) error {
	var c *rpc.Client
	select {
	case c = <-p.free:
	case <-time.After(p.checkoutTimeout):
		return ErrPoolExhausted
	}
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

func newPooledCaller(addrs []string, size int, checkoutTimeout time.Duration) (*pooledCaller, error) {
	pools := make(map[string]*Pool, len(addrs))
	for _, addr := range addrs {
		p, err := NewPool(addr, size, checkoutTimeout)
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

// defaultCheckoutTimeout is what NewPooledClient uses when a caller
// doesn't need to tune it directly (NewPool itself always takes an
// explicit one — see e.g. the backpressure tests, which need a much
// shorter timeout to stay fast). Derived from raft.HeartbeatInterval,
// the same constant client.go's own retry loop paces its sleep against,
// rather than an unrelated new magic number: comfortably longer than a
// single heartbeat round (so a short, real burst has a real chance to
// drain) while staying well under client.go's own outer retry cycle
// (so an exhausted pool doesn't dominate a request's total latency
// before the caller's retry loop gets a chance to try a different
// server instead).
const defaultCheckoutTimeout = 10 * raft.HeartbeatInterval

// NewPooledClient returns a PooledClient addressing any of addrs, with
// poolSize connections pre-dialed to EACH address and
// defaultCheckoutTimeout as every pool's backpressure bound (see
// Pool's own doc comment). Call Close when done to release every
// underlying connection.
func NewPooledClient(addrs []string, poolSize int) (*PooledClient, error) {
	pc, err := newPooledCaller(addrs, poolSize, defaultCheckoutTimeout)
	if err != nil {
		return nil, err
	}
	return &PooledClient{client: newClient(addrs, pc), pooled: pc}, nil
}

// Close releases every connection this client has pooled.
func (c *PooledClient) Close() error {
	return c.pooled.close()
}
