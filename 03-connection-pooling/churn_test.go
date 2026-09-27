package pool

import (
	"errors"
	"net/rpc"
	"sync"
	"testing"
	"time"

	kvstore "github.com/aksharraikanti/distsys-lab/02-kv-store"
)

// TestPoolNeverHoldsMoreConnectionsThanMaxSize is a regression test for a bug
// Stage 4's fault test exposed. The redial paths (evictAndReplace and
// redialUntilMinRestored) used to CHECK "is count below MinSize?", then dial
// with the lock released, then increment — so several concurrent redialers,
// plus on-demand growth refilling the same gap, could each pass the check and
// push count past MaxSize. free's capacity is MaxSize, so the surplus
// connection's send into it blocked forever: a stuck evictor, a stuck checkin,
// and a Close that never returned.
//
// The fix reserves the slot (under the lock) BEFORE dialing, exactly as growth
// already did, so count can never exceed MaxSize by construction. This test
// hammers that interleaving: an endpoint that keeps crashing and restarting
// (so connections keep breaking and redials keep failing then succeeding)
// under concurrent Calls that keep growing the pool, with a sampler asserting
// the invariant continuously and Close required to return.
func TestPoolNeverHoldsMoreConnectionsThanMaxSize(t *testing.T) {
	const minSize, maxSize = 1, 4

	_, eps, _, addrs, cleanup := faultyTCPCluster(t, 1)
	defer cleanup()
	ep := eps[0]

	opts := PoolOptions{
		MinSize:         minSize,
		MaxSize:         maxSize,
		CheckoutTimeout: 20 * time.Millisecond,
		RedialInterval:  2 * time.Millisecond,
		IdleTimeout:     time.Hour,
	}
	p, err := NewPool(addrs[0], opts)
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Callers: constant demand, so the pool keeps growing toward MaxSize.
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				var reply kvstore.GetReply
				_ = p.Call("KVServer.Get", &kvstore.GetArgs{Key: "x"}, &reply) // errors are expected mid-churn
			}
		}()
	}

	// Churn: crash and restart the endpoint, severing every connection.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(8 * time.Millisecond):
			}
			ep.crash()
			time.Sleep(6 * time.Millisecond)
			if err := ep.start(); err != nil {
				t.Errorf("restart: %v", err)
				return
			}
		}
	}()

	// Sampler: the invariant, checked continuously rather than once at the end.
	violated := make(chan int, 1)
	maxSeen := 0
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Microsecond):
			}
			c := poolCount(p)
			if c > maxSeen {
				maxSeen = c
			}
			if c > maxSize {
				select {
				case violated <- c:
				default:
				}
				return
			}
		}
	}()

	time.Sleep(1500 * time.Millisecond)
	close(stop)
	wg.Wait()
	if err := ep.start(); err != nil && ep.l == nil {
		_ = err // already running is fine; the point is the endpoint is up for Close
	}

	t.Logf("max count observed: %d (MaxSize %d)", maxSeen, maxSize)
	select {
	case c := <-violated:
		t.Errorf("pool held %d connections, more than MaxSize %d", c, maxSize)
	default:
	}

	closed := make(chan struct{})
	go func() { p.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Pool.Close() never returned: a background goroutine is stuck (most likely blocked sending into a full free list)")
	}
}

// TestRedialCannotPushCountPastMaxSize forces the interleaving the stress test
// above only hopes to hit. A background redialer passes its "is count below
// MinSize?" check, then is parked mid-dial; meanwhile on-demand growth fills
// the pool to MaxSize; then the redialer's dial completes. If the redialer
// only increments count AFTER dialing (check-then-act), count ends at
// MaxSize+1 — more connections than free's capacity — which is exactly the
// state in which a checkin or the idle evictor blocks forever on a full free
// list. Reserving the slot BEFORE dialing makes that impossible.
func TestRedialCannotPushCountPastMaxSize(t *testing.T) {
	const minSize, maxSize = 1, 2
	addr, l, cleanup := singleNodeKVServer(t, "127.0.0.1:0")
	defer l.Close()
	defer cleanup()

	p, err := NewPool(addr, PoolOptions{
		MinSize:         minSize,
		MaxSize:         maxSize,
		CheckoutTimeout: 40 * time.Millisecond,
		RedialInterval:  time.Millisecond,
		IdleTimeout:     time.Hour,
	})
	if err != nil {
		t.Fatalf("NewPool: %v", err)
	}

	var mu sync.Mutex
	failing, gated := true, false
	entered := make(chan struct{}, 8)
	gate := make(chan struct{})
	p.dial = func() (*rpc.Client, error) {
		mu.Lock()
		f, g := failing, gated
		mu.Unlock()
		if f {
			return nil, errors.New("simulated: node down")
		}
		if g {
			entered <- struct{}{}
			<-gate
		}
		return rpc.Dial("tcp", addr)
	}

	// 1. Break the pool's only connection while dials fail: count drops to 0,
	//    the immediate redial fails, and a background redialer is spawned.
	held := <-p.free
	held.client.Close()
	p.free <- held
	var reply kvstore.GetReply
	if err := p.Call("KVServer.Get", &kvstore.GetArgs{Key: "x"}, &reply); err == nil {
		t.Fatal("Call on a closed connection should have failed")
	}
	if got := poolCount(p); got != 0 {
		t.Fatalf("count = %d after evicting the only connection, want 0", got)
	}

	// 2. The node comes back, but dials now hang: the redialer's next tick
	//    passes its check and parks inside dial.
	mu.Lock()
	failing, gated = false, true
	mu.Unlock()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the background redialer never started dialing")
	}

	// 3. While it's parked, growth fills the pool. Dials no longer hang for
	//    callers, so these complete immediately.
	mu.Lock()
	gated = false
	mu.Unlock()
	var out []*rpc.Client
	for i := 0; i < maxSize; i++ {
		if c, err := p.checkout(); err == nil {
			out = append(out, c)
		}
	}

	// 4. Release the parked redialer and let it finish.
	close(gate)
	waitFor(t, 5*time.Second, func() bool { return len(p.free) >= 1 })

	if got := poolCount(p); got > maxSize {
		t.Errorf("count = %d > MaxSize %d: a redialer that checks count and increments only after dialing lets the pool exceed its own capacity", got, maxSize)
	}
	for _, c := range out {
		p.checkin(c)
	}

	closed := make(chan struct{})
	go func() { p.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Pool.Close() never returned")
	}
}
