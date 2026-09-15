package kvstore

import (
	"crypto/rand"
	"math/big"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
)

// Clerk is a client of the KV cluster. It has no way to know which
// server is currently the leader, so every call tries servers in order
// — starting from whichever one last worked — until one succeeds,
// retrying indefinitely on ErrWrongLeader/ErrTimeout. As long as a
// majority of the cluster is alive and can communicate, this is
// guaranteed to eventually make progress; it does not (and cannot)
// bound how long that might take.
//
// Each Clerk generates its own ClientID once, at construction, and
// increments a SeqNum per new logical request — this is the exact
// ClientID/SeqNum contract Day 3's dedup logic depends on. A retried
// call (ErrWrongLeader/ErrTimeout from one server) resends the SAME
// args, SAME SeqNum, to the next server; that resend IS the retry Day
// 3 exists to make safe.
//
// ClientID is drawn from crypto/rand rather than a math/rand source
// seeded off time.Now().UnixNano(): two Clerks constructed within the
// same wall-clock nanosecond (routine when a test launches several
// client goroutines back to back) would otherwise seed identically and
// draw the same "random" ClientID. Two live Clerks sharing a ClientID
// corrupts the duplicateTable's per-ClientID SeqNum tracking — one
// client's write gets silently treated as an already-seen duplicate of
// the other's and its mutation is skipped, yet PutAppend still reports
// OK, because the notify channel fires unconditionally on the proposed
// op matching, regardless of whether the dedup check let the mutation
// through. This was found the hard way via Day 5's stress tests.
//
// A single Clerk is NOT safe for concurrent use by multiple goroutines
// — its SeqNum counter and lastKnown-server hint aren't synchronized
// against overlapping in-flight calls. Day 5's stress test gives each
// simulated client its own Clerk for exactly this reason.
type Clerk struct {
	servers  []*KVServer
	clientID int64
	seqNum   int64

	lastKnown int // index into servers of the last one that worked
}

// NewClerk returns a Clerk that will address any of servers.
func NewClerk(servers []*KVServer) *Clerk {
	n, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		panic("kvstore: failed to generate ClientID: " + err.Error())
	}
	return &Clerk{
		servers:  servers,
		clientID: n.Int64(),
	}
}

// Get fetches the current value for key, or "" if the key has never
// been set. Blocks until some server answers definitively.
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{Key: key}
	for {
		for i := 0; i < len(ck.servers); i++ {
			idx := (ck.lastKnown + i) % len(ck.servers)
			var reply GetReply
			if err := ck.servers[idx].Get(args, &reply); err != nil {
				continue
			}
			switch reply.Err {
			case OK:
				ck.lastKnown = idx
				return reply.Value
			case ErrNoKey:
				ck.lastKnown = idx
				return ""
			}
			// ErrWrongLeader: try the next server.
		}
		// No server in the cluster answered — most likely an election
		// is in progress. Wait a moment rather than spinning, then try
		// the whole cluster again.
		time.Sleep(raft.HeartbeatInterval)
	}
}

// Put sets key to value.
func (ck *Clerk) Put(key, value string) { ck.putAppend(key, value, "Put") }

// Append appends value onto whatever key currently holds.
func (ck *Clerk) Append(key, value string) { ck.putAppend(key, value, "Append") }

func (ck *Clerk) putAppend(key, value, op string) {
	ck.seqNum++
	args := &PutAppendArgs{Key: key, Value: value, Op: op, ClientID: ck.clientID, SeqNum: ck.seqNum}
	for {
		for i := 0; i < len(ck.servers); i++ {
			idx := (ck.lastKnown + i) % len(ck.servers)
			var reply PutAppendReply
			if err := ck.servers[idx].PutAppend(args, &reply); err != nil {
				continue
			}
			if reply.Err == OK {
				ck.lastKnown = idx
				return
			}
			// ErrWrongLeader or ErrTimeout: try the next server, same args.
		}
		time.Sleep(raft.HeartbeatInterval)
	}
}
