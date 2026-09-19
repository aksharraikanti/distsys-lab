package pool

import (
	"crypto/rand"
	"math/big"
	"net/rpc"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
	kvstore "github.com/aksharraikanti/distsys-lab/02-kv-store"
)

// NaiveClient talks to a KVServer cluster over real TCP, dialing a
// fresh *rpc.Client for every single call and closing it immediately
// after — no reuse, no pooling. This is Stage 3's deliberate starting
// point, not an oversight: it's the cold-start baseline every later
// day's pooling has to beat with a real, measured number (see
// BenchmarkNaiveClientPutAppend), not just "pooling should be faster."
//
// Otherwise mirrors 02-kv-store's own Clerk almost exactly: doesn't
// know which server currently leads, retries round-robin starting from
// whichever one last worked, and uses ClientID+SeqNum for the same
// idempotent-retry contract Stage 2 Day 3 built. The one real
// difference is what counts as "try the next server instead" — Clerk's
// in-process calls only ever fail via reply.Err; NaiveClient's dial or
// RPC call can ALSO fail outright (connection refused, timeout, a node
// that's down or not listening yet), a failure mode that plainly
// couldn't exist without a real socket in between.
type NaiveClient struct {
	addrs     []string
	clientID  int64
	seqNum    int64
	lastKnown int
}

// NewNaiveClient returns a NaiveClient that will address any of addrs
// (each a "host:port" a KVServer is being served on via ServeKVServer).
func NewNaiveClient(addrs []string) *NaiveClient {
	n, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		panic("pool: failed to generate ClientID: " + err.Error())
	}
	return &NaiveClient{addrs: addrs, clientID: n.Int64()}
}

// call dials addr fresh, makes exactly one RPC, and closes the
// connection — the entire "no pooling" story in one place. Every
// later day's pooled client replaces just this method.
func (c *NaiveClient) call(addr, method string, args, reply interface{}) error {
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer client.Close()
	return client.Call(method, args, reply)
}

// Get fetches the current value for key, or "" if the key has never
// been set. Blocks until some server answers definitively.
func (c *NaiveClient) Get(key string) string {
	args := &kvstore.GetArgs{Key: key}
	for {
		for i := 0; i < len(c.addrs); i++ {
			idx := (c.lastKnown + i) % len(c.addrs)
			var reply kvstore.GetReply
			if err := c.call(c.addrs[idx], "KVServer.Get", args, &reply); err != nil {
				continue
			}
			switch reply.Err {
			case kvstore.OK:
				c.lastKnown = idx
				return reply.Value
			case kvstore.ErrNoKey:
				c.lastKnown = idx
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
func (c *NaiveClient) Put(key, value string) { c.putAppend(key, value, "Put") }

// Append appends value onto whatever key currently holds.
func (c *NaiveClient) Append(key, value string) { c.putAppend(key, value, "Append") }

func (c *NaiveClient) putAppend(key, value, op string) {
	c.seqNum++
	args := &kvstore.PutAppendArgs{Key: key, Value: value, Op: op, ClientID: c.clientID, SeqNum: c.seqNum}
	for {
		for i := 0; i < len(c.addrs); i++ {
			idx := (c.lastKnown + i) % len(c.addrs)
			var reply kvstore.PutAppendReply
			if err := c.call(c.addrs[idx], "KVServer.PutAppend", args, &reply); err != nil {
				continue
			}
			if reply.Err == kvstore.OK {
				c.lastKnown = idx
				return
			}
			// ErrWrongLeader or ErrTimeout: try the next server, same args.
		}
		time.Sleep(raft.HeartbeatInterval)
	}
}
