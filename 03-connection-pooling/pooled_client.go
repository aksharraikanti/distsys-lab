package pool

import (
	"errors"
	"fmt"
	"net/rpc"
	"sync"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
)

// ErrPoolExhausted is returned by Call when every connection in the
// pool was still checked out by some other concurrent caller for the
// entire checkoutTimeout, AND the pool had already grown to MaxSize —
// see Pool's own doc comment for why a bounded wait, not an unbounded
// block or an immediate rejection, is Day 3's deliberate choice.
var ErrPoolExhausted = errors.New("pool: no connection became available before the checkout timeout")

// PoolOptions configures a Pool's elastic sizing (Day 5) and timing
// behavior. Grouped into a struct rather than more positional
// parameters on NewPool because that parameter list has grown, day by
// day, to the point where several same-typed (time.Duration)
// parameters in a row would be genuinely easy to swap by accident at a
// call site — a real risk once there are three of them, not a
// hypothetical one.
type PoolOptions struct {
	// MinSize/MaxSize bound how many connections the pool keeps open at
	// once. MinSize are dialed up front and never evicted for being
	// idle (though a broken one is still replaced — see
	// evictAndReplace); the pool can grow past that, on demand, up to
	// MaxSize under real concurrent load, and shrinks back down toward
	// MinSize once that load passes and connections sit idle past
	// IdleTimeout. MinSize may be 0 (a fully on-demand pool); MaxSize
	// must be at least 1 and at least MinSize.
	MinSize int
	MaxSize int

	// CheckoutTimeout bounds how long Call waits for a connection when
	// the pool is already at MaxSize and every connection is checked
	// out — see ErrPoolExhausted and Pool's own doc comment (Day 3).
	CheckoutTimeout time.Duration
	// RedialInterval paces how often a broken connection below MinSize
	// is retried after an immediate redial attempt fails (Day 4).
	RedialInterval time.Duration
	// IdleTimeout is how long a connection above MinSize can sit unused
	// in the free list before it's closed and not replaced (Day 5).
	IdleTimeout time.Duration
}

// Pool is an elastic set of already-dialed *rpc.Client connections to
// ONE server address, checked out before a call and checked back in
// after — a request reuses an existing connection instead of paying a
// fresh TCP handshake every time (see NaiveClient/
// BenchmarkNaiveClientPutAppend for the cold-start cost this exists to
// avoid).
//
// Sizing is elastic between MinSize and MaxSize (Day 5), not a single
// fixed number: MinSize connections are always kept ready (topped back
// up if one breaks — see evictAndReplace), the pool grows past that on
// demand up to MaxSize when concurrent load actually calls for it, and
// shrinks back down toward MinSize once connections sit idle past
// IdleTimeout — reuse-vs-cold-start cuts both ways, and an idle
// connection held open forever wastes resources on both ends, not just
// the client's.
//
// More concurrent callers than the pool currently has connections for
// (whether or not it's reached MaxSize yet) is the NORMAL case under
// real load, not an edge case — CheckoutTimeout (Day 3) is the
// deliberate policy once growth room runs out too: Call blocks until a
// connection frees up, but only up to CheckoutTimeout, then returns
// ErrPoolExhausted. Two other policies were on the table and rejected:
// an unbounded overflow queue just moves the problem (unbounded memory
// growth instead of blocked goroutines) without actually bounding wait
// time; rejecting immediately treats "busy right now" the same as
// "actually broken," which would make a routine, short-lived burst fail
// requests it could easily have absorbed. A bounded wait gives a real
// burst a real chance to drain while still failing fast enough that the
// caller's own retry loop (client.go) can fall back to a different
// server rather than hang behind this one indefinitely.
//
// Day 4 adds detecting and replacing a connection that's gone bad, not
// just busy (the KV node behind it crashed or restarted — Stage 2's own
// fault injection is exactly this scenario). stopCh/wg exist for that
// AND for Day 5's idle evictor: both run as background goroutines, and
// Close needs to know about and wait for them before it drains/closes
// free, or one could try to send on a closed channel.
type Pool struct {
	addr string
	opts PoolOptions

	free chan *pooledConn // buffered at MaxSize capacity

	mu    sync.Mutex // protects count
	count int        // total live connections: checked out + in free

	stopCh   chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// pooledConn pairs a connection with when it was last checked back
// into the free list — the timestamp Day 5's idle evictor needs to
// decide whether it's been sitting unused long enough to close.
type pooledConn struct {
	client     *rpc.Client
	returnedAt time.Time
}

// NewPool dials opts.MinSize connections to addr up front and returns
// a Pool ready to hand them out and grow/shrink within opts' bounds —
// see PoolOptions and Pool's own doc comments. If any of the initial
// dials fails, every connection already established is closed before
// returning the error — a partially initialized pool would be a silent
// resource leak, not a smaller pool.
func NewPool(addr string, opts PoolOptions) (*Pool, error) {
	if opts.MaxSize < 1 {
		return nil, fmt.Errorf("pool: MaxSize must be at least 1, got %d", opts.MaxSize)
	}
	if opts.MinSize < 0 || opts.MinSize > opts.MaxSize {
		return nil, fmt.Errorf("pool: MinSize (%d) must be between 0 and MaxSize (%d)", opts.MinSize, opts.MaxSize)
	}

	p := &Pool{
		addr:   addr,
		opts:   opts,
		free:   make(chan *pooledConn, opts.MaxSize),
		stopCh: make(chan struct{}),
	}
	for i := 0; i < opts.MinSize; i++ {
		c, err := rpc.Dial("tcp", addr)
		if err != nil {
			for len(p.free) > 0 {
				(<-p.free).client.Close()
			}
			return nil, fmt.Errorf("pool: dialing connection %d/%d to %s: %w", i+1, opts.MinSize, addr, err)
		}
		p.count++
		p.free <- &pooledConn{client: c, returnedAt: time.Now()}
	}

	p.wg.Add(1)
	go p.runIdleEvictor()
	return p, nil
}

// Call checks out a connection — reusing an idle one, growing the pool
// if there's room and none is idle (see checkout), or waiting up to
// CheckoutTimeout if the pool is already at MaxSize (Day 3) — and makes
// the RPC. A connection that served the call successfully, at the
// TRANSPORT level, goes back to the free list; one that failed is
// evicted and replaced instead (see evictAndReplace). The error from
// THIS call is still returned to the caller either way, so a failure is
// never silently swallowed.
//
// Distinguishing "transport failure" from "application-level outcome"
// needs no special-casing here: every RPC this pool ever makes
// (KVServer.Get/PutAppend) always returns a nil Go error — the actual
// outcome (success, wrong leader, no such key, ...) travels through
// reply.Err instead (see 02-kv-store/rpc.go's Err type). So any
// non-nil error c.Call itself returns is necessarily a transport-level
// problem — a dropped connection, the node behind it having crashed or
// restarted — never a "wrong leader" outcome, which never surfaces as a
// Go error at all.
func (p *Pool) Call(method string, args, reply interface{}) error {
	c, err := p.checkout()
	if err != nil {
		return err
	}

	if err := c.Call(method, args, reply); err != nil {
		p.evictAndReplace(c)
		return err
	}
	p.checkin(c)
	return nil
}

// checkout returns an existing idle connection if one is available
// right now; otherwise, if the pool hasn't reached MaxSize yet, dials a
// fresh one on the spot rather than making this caller wait behind
// others when there was room to grow; otherwise waits up to
// CheckoutTimeout for one to free up (Day 3).
func (p *Pool) checkout() (*rpc.Client, error) {
	select {
	case pc := <-p.free:
		return pc.client, nil
	default:
	}

	if p.tryReserveGrowth() {
		c, err := rpc.Dial("tcp", p.addr)
		if err == nil {
			return c, nil
		}
		// The node might just be down — fall through to waiting for an
		// existing connection instead of failing this call outright;
		// releasing the reservation lets a LATER checkout try growing
		// again once the node is reachable.
		p.releaseGrowthReservation()
	}

	select {
	case pc := <-p.free:
		return pc.client, nil
	case <-time.After(p.opts.CheckoutTimeout):
		return nil, ErrPoolExhausted
	}
}

// checkin returns a healthy connection to the free list. free's
// capacity is always MaxSize and count never exceeds MaxSize (every
// grower reserves its slot first via tryReserveGrowth), so this send
// can never block.
func (p *Pool) checkin(c *rpc.Client) {
	p.free <- &pooledConn{client: c, returnedAt: time.Now()}
}

func (p *Pool) tryReserveGrowth() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.count < p.opts.MaxSize {
		p.count++
		return true
	}
	return false
}

func (p *Pool) releaseGrowthReservation() {
	p.mu.Lock()
	p.count--
	p.mu.Unlock()
}

// evictAndReplace closes a connection that just failed. Above MinSize,
// that's all it does — the pool simply shrinks by one, exactly like an
// idle eviction would, and grows back via checkout's own on-demand path
// if load actually calls for it again (Day 5 made this the right
// default; under Day 4 alone, with no elasticity, every broken
// connection HAD to be replaced immediately to avoid a permanent
// shortfall). At or below MinSize, though, this pool has promised to
// keep that many connections ready, so it tries an IMMEDIATE redial
// first (the common case — a stale connection to a node that's actually
// fine); if that also fails (the node is genuinely still down), a
// background goroutine keeps retrying every RedialInterval — WITHOUT
// blocking this or any other caller — until the minimum is restored or
// Close stops it.
func (p *Pool) evictAndReplace(broken *rpc.Client) {
	broken.Close()
	p.mu.Lock()
	p.count--
	belowMin := p.count < p.opts.MinSize
	p.mu.Unlock()
	if !belowMin {
		return
	}

	if c, err := rpc.Dial("tcp", p.addr); err == nil {
		p.mu.Lock()
		p.count++
		p.mu.Unlock()
		p.checkin(c)
		return
	}
	p.wg.Add(1)
	go p.redialUntilMinRestored()
}

func (p *Pool) redialUntilMinRestored() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.opts.RedialInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.mu.Lock()
			stillBelowMin := p.count < p.opts.MinSize
			p.mu.Unlock()
			if !stillBelowMin {
				// Something else (another eviction's own redial, or a
				// concurrent checkout's growth) already restored the
				// minimum — nothing left for this goroutine to do.
				return
			}
			c, err := rpc.Dial("tcp", p.addr)
			if err != nil {
				continue
			}
			p.mu.Lock()
			p.count++
			p.mu.Unlock()
			select {
			case p.free <- &pooledConn{client: c, returnedAt: time.Now()}:
			case <-p.stopCh:
				p.mu.Lock()
				p.count--
				p.mu.Unlock()
				c.Close()
			}
			return
		}
	}
}

// runIdleEvictor periodically sweeps the free list and closes any
// connection above MinSize that's sat unused past IdleTimeout — Day
// 5's half of "reuse-vs-cold-start cuts both ways." Checked at
// idleCheckInterval, a fraction of IdleTimeout itself, so a connection
// doesn't sit evictable for a long time before anything actually
// notices.
func (p *Pool) runIdleEvictor() {
	defer p.wg.Done()
	ticker := time.NewTicker(p.idleCheckInterval())
	defer ticker.Stop()
	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.evictIdleConnections()
		}
	}
}

func (p *Pool) idleCheckInterval() time.Duration {
	interval := p.opts.IdleTimeout / 4
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	return interval
}

// evictIdleConnections drains the free list once, keeping every
// connection still worth keeping and closing the rest. Bounded by
// len(p.free) at the moment it starts, so a steady stream of concurrent
// checkins can't make this loop run indefinitely.
func (p *Pool) evictIdleConnections() {
	n := len(p.free)
	for i := 0; i < n; i++ {
		var pc *pooledConn
		select {
		case pc = <-p.free:
		default:
			return // drained concurrently by something else; done early
		}

		p.mu.Lock()
		shouldEvict := p.count > p.opts.MinSize && time.Since(pc.returnedAt) > p.opts.IdleTimeout
		if shouldEvict {
			p.count--
		}
		p.mu.Unlock()

		if shouldEvict {
			pc.client.Close()
		} else {
			p.free <- pc
		}
	}
}

// Close closes every connection currently in the pool. Callers must
// not still have a connection checked out via Call when Close runs —
// the same "no more calls in flight" precondition every Stop/Close in
// this codebase already relies on its caller to uphold (see
// 02-kv-store's KVServer.Stop, 01-raft's StopElectionTimer). Stops the
// idle evictor and any in-flight redialUntilMinRestored goroutine and
// waits for them to actually exit BEFORE draining/closing free —
// otherwise one could try to send on an already-closed channel.
func (p *Pool) Close() error {
	p.stopOnce.Do(func() { close(p.stopCh) })
	p.wg.Wait()

	close(p.free)
	var firstErr error
	for pc := range p.free {
		if err := pc.client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// pooledCaller routes each call through the Pool for that specific
// address — one elastic Pool per address. Most connections sit idle at
// any given moment, since only the current leader actually answers a
// call with OK, but MinSize per address are always kept ready so a
// leadership change doesn't have to pay a fresh dial right when it's
// needed most.
type pooledCaller struct {
	pools map[string]*Pool
}

func newPooledCaller(addrs []string, opts PoolOptions) (*pooledCaller, error) {
	pools := make(map[string]*Pool, len(addrs))
	for _, addr := range addrs {
		p, err := NewPool(addr, opts)
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
// NaiveClient uses (see client.go's shared client type), backed by an
// elastic Pool per server address instead of a fresh dial on every
// call.
type PooledClient struct {
	*client
	pooled *pooledCaller
}

// defaultCheckoutTimeout is one field of defaultPoolOptions — see that
// var's own doc comment. Derived from raft.HeartbeatInterval, the same
// constant client.go's own retry loop paces its sleep against, rather
// than an unrelated new magic number: comfortably longer than a single
// heartbeat round (so a short, real burst has a real chance to drain)
// while staying well under client.go's own outer retry cycle (so an
// exhausted pool doesn't dominate a request's total latency before the
// caller's retry loop gets a chance to try a different server instead).
const defaultCheckoutTimeout = 10 * raft.HeartbeatInterval

// defaultRedialInterval paces how often a Pool retries restoring MinSize
// after an immediate redial attempt fails (Day 4). Derived from
// raft.ElectionTimeoutMin rather than an unrelated new number: fast
// enough that the pool recovers promptly once a crashed node comes
// back — Stage 2's own fault-injection tests typically hold a node down
// for a couple of raft.ElectionTimeoutMax, so several retries fit
// comfortably inside that window — without hammering a node that's
// still genuinely down with a new connection attempt every couple of
// milliseconds.
const defaultRedialInterval = raft.ElectionTimeoutMin

// defaultIdleTimeout (Day 5) is how long an above-MinSize connection
// sits unused before NewPooledClient's pools close it. Derived from
// raft.ElectionTimeoutMax: long enough that a connection isn't closed
// and redialed on the very next request during an ordinary lull between
// bursts, short enough that a real idle period (the client genuinely
// stopped needing this many connections) doesn't hold sockets open on
// both ends for no reason.
const defaultIdleTimeout = 20 * raft.ElectionTimeoutMax

// defaultPoolOptions fills in every PoolOptions field EXCEPT
// MinSize/MaxSize, which depend on how many connections a specific
// caller actually wants per address — see NewPooledClient.
var defaultPoolOptions = PoolOptions{
	CheckoutTimeout: defaultCheckoutTimeout,
	RedialInterval:  defaultRedialInterval,
	IdleTimeout:     defaultIdleTimeout,
}

// NewPooledClient returns a PooledClient addressing any of addrs, with
// each address's Pool bounded between minSize and maxSize connections
// (see PoolOptions) and every other option at its documented default.
// Call Close when done to release every underlying connection.
func NewPooledClient(addrs []string, minSize, maxSize int) (*PooledClient, error) {
	opts := defaultPoolOptions
	opts.MinSize = minSize
	opts.MaxSize = maxSize
	pc, err := newPooledCaller(addrs, opts)
	if err != nil {
		return nil, err
	}
	return &PooledClient{client: newClient(addrs, pc), pooled: pc}, nil
}

// Close releases every connection this client has pooled.
func (c *PooledClient) Close() error {
	return c.pooled.close()
}
