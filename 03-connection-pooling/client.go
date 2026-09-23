package pool

import (
	"crypto/rand"
	"math/big"
	"net/rpc"
	"sync"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
	kvstore "github.com/aksharraikanti/distsys-lab/02-kv-store"
)

// caller is the one thing that actually differs between NaiveClient
// (Day 1) and PooledClient (Day 2): HOW a single RPC to addr gets
// made. Everything else — round-robin server selection starting from
// whichever one last worked, retry on ErrWrongLeader, ClientID+SeqNum
// dedup — has nothing to do with the transport underneath it, which is
// exactly what Day 1's own README note observed once NaiveClient
// turned out nearly line-for-line identical to 02-kv-store's Clerk.
// Factored out here rather than duplicated a second time for
// PooledClient.
type caller interface {
	call(addr, method string, args, reply interface{}) error
}

// client is the shared retry/dedup machinery every KV client in this
// stage builds on — see caller's doc comment for why this exists as
// its own type instead of being copy-pasted per client kind. Not
// exported: NaiveClient and PooledClient each embed it and are the
// actual public API, the same way neither concrete type needs to leak
// which caller it's using.
//
// A client IS safe for concurrent use (Stage 4 needed that: a cache in
// front of it is naturally one shared client with many callers; until
// then every caller had its own, and this documented the opposite).
// Reads run concurrently. Writes are SERIALIZED, and that is a
// correctness requirement, not a convenience: 02-kv-store's dedup keeps
// only the HIGHEST SeqNum applied per ClientID, so if one client had
// seq 4 and seq 5 in flight at once and 5 committed first, 4 would then
// be dropped as an already-seen "duplicate" — an acknowledged write
// silently lost. One write at a time per ClientID is what the protocol
// assumes. (Write throughput through one client is therefore serial;
// several clients in parallel is how to get more.)
type client struct {
	addrs    []string
	caller   caller
	clientID int64

	writeMu sync.Mutex // serializes putAppend; also protects seqNum
	seqNum  int64

	mu        sync.Mutex // protects lastKnown
	lastKnown int
}

func (c *client) startIndex() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastKnown
}

func (c *client) setLastKnown(idx int) {
	c.mu.Lock()
	c.lastKnown = idx
	c.mu.Unlock()
}

// newClient returns a client that will address any of addrs, making
// each individual RPC through c.
func newClient(addrs []string, c caller) *client {
	n, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		panic("pool: failed to generate ClientID: " + err.Error())
	}
	return &client{addrs: addrs, caller: c, clientID: n.Int64()}
}

// Get fetches the current value for key, or "" if the key has never
// been set. Blocks until some server answers definitively.
func (c *client) Get(key string) string {
	args := &kvstore.GetArgs{Key: key}
	for {
		start := c.startIndex()
		for i := 0; i < len(c.addrs); i++ {
			idx := (start + i) % len(c.addrs)
			var reply kvstore.GetReply
			if err := c.caller.call(c.addrs[idx], "KVServer.Get", args, &reply); err != nil {
				continue
			}
			switch reply.Err {
			case kvstore.OK:
				c.setLastKnown(idx)
				return reply.Value
			case kvstore.ErrNoKey:
				c.setLastKnown(idx)
				return ""
			}
			// ErrWrongLeader: try the next server.
		}
		// No server answered definitively — most likely an election in
		// progress, or every remaining candidate is unreachable right
		// now. Wait a moment rather than spinning, then try again.
		time.Sleep(raft.HeartbeatInterval)
	}
}

// Put sets key to value.
func (c *client) Put(key, value string) { c.putAppend(key, value, "Put") }

// Append appends value onto whatever key currently holds.
func (c *client) Append(key, value string) { c.putAppend(key, value, "Append") }

func (c *client) putAppend(key, value, op string) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.seqNum++
	args := &kvstore.PutAppendArgs{Key: key, Value: value, Op: op, ClientID: c.clientID, SeqNum: c.seqNum}
	for {
		start := c.startIndex()
		for i := 0; i < len(c.addrs); i++ {
			idx := (start + i) % len(c.addrs)
			var reply kvstore.PutAppendReply
			if err := c.caller.call(c.addrs[idx], "KVServer.PutAppend", args, &reply); err != nil {
				continue
			}
			if reply.Err == kvstore.OK {
				c.setLastKnown(idx)
				return
			}
			// ErrWrongLeader or ErrTimeout: try the next server, same args.
		}
		time.Sleep(raft.HeartbeatInterval)
	}
}

// NaiveClient talks to a KVServer cluster over real TCP, dialing a
// fresh *rpc.Client for every single call and closing it immediately
// after — no reuse, no pooling. This is Stage 3's deliberate starting
// point, not an oversight: it's the cold-start baseline every later
// day's pooling has to beat with a real, measured number (see
// BenchmarkNaiveClientPutAppend), not just "pooling should be faster."
type NaiveClient struct {
	*client
}

// NewNaiveClient returns a NaiveClient that will address any of addrs
// (each a "host:port" a KVServer is being served on via ServeKVServer).
func NewNaiveClient(addrs []string) *NaiveClient {
	return &NaiveClient{client: newClient(addrs, dialPerCallCaller{})}
}

// dialPerCallCaller dials addr fresh, makes exactly one RPC, and
// closes the connection — the entire "no pooling" story in one place.
type dialPerCallCaller struct{}

func (dialPerCallCaller) call(addr, method string, args, reply interface{}) error {
	c, err := rpc.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer c.Close()
	return c.Call(method, args, reply)
}
