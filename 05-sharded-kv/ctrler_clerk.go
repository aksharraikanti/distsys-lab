package shardkv

import (
	"crypto/rand"
	"math/big"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
)

// CtrlerClerk is a client of the shard controller — 02-kv-store's Clerk,
// retargeted at Ctrler's RPCs instead of KVServer's. Same contract: it
// doesn't know which server is leader, so every call round-robins through
// servers (starting from whichever one last worked) until one succeeds,
// retrying indefinitely on ErrWrongLeader/ErrTimeout; ClientID is drawn from
// crypto/rand for the same reason Clerk's is (see Clerk's doc comment); and
// a single CtrlerClerk is NOT safe for concurrent use, for the same reason
// (an unsynchronized seqNum counter and lastKnown hint).
//
// Calls servers in-process (*Ctrler method calls, not RPC over a socket) —
// exactly how Clerk talked to KVServer before Stage 3 put a real network
// boundary in front of it. Day 4 is where groups start actually polling the
// controller across process boundaries; giving Ctrler a real net/rpc face
// at that point, mirroring 03-connection-pooling's ServeKVServer, is that
// day's job, not this one's — this day is about the replicated log being
// correct, which an in-process client is enough to prove.
type CtrlerClerk struct {
	servers  []*Ctrler
	clientID int64
	seqNum   int64

	lastKnown int
}

// NewCtrlerClerk returns a CtrlerClerk that will address any of servers.
func NewCtrlerClerk(servers []*Ctrler) *CtrlerClerk {
	n, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		panic("shardkv: failed to generate ClientID: " + err.Error())
	}
	return &CtrlerClerk{servers: servers, clientID: n.Int64()}
}

// Join adds (or updates) the given groups and waits for the resulting
// rebalance to commit.
func (ck *CtrlerClerk) Join(groups map[int][]string) {
	ck.seqNum++
	args := &JoinArgs{Groups: groups, ClientID: ck.clientID, SeqNum: ck.seqNum}
	for {
		for i := 0; i < len(ck.servers); i++ {
			idx := (ck.lastKnown + i) % len(ck.servers)
			var reply JoinReply
			if err := ck.servers[idx].Join(args, &reply); err != nil {
				continue
			}
			if reply.Err == CtrlerOK {
				ck.lastKnown = idx
				return
			}
			// ErrWrongLeader or ErrTimeout: try the next server, same args.
		}
		time.Sleep(raft.HeartbeatInterval)
	}
}

// Leave removes the given group ids and waits for the resulting rebalance
// to commit.
func (ck *CtrlerClerk) Leave(gids []int) {
	ck.seqNum++
	args := &LeaveArgs{GIDs: gids, ClientID: ck.clientID, SeqNum: ck.seqNum}
	for {
		for i := 0; i < len(ck.servers); i++ {
			idx := (ck.lastKnown + i) % len(ck.servers)
			var reply LeaveReply
			if err := ck.servers[idx].Leave(args, &reply); err != nil {
				continue
			}
			if reply.Err == CtrlerOK {
				ck.lastKnown = idx
				return
			}
		}
		time.Sleep(raft.HeartbeatInterval)
	}
}

// Move reassigns shard to gid and waits for it to commit. Panics if the
// controller rejects it as malformed (CtrlerErrInvalidArgs) — a bad shard
// number or unknown gid is a caller bug, not a transient condition retrying
// would ever resolve, so retrying it forever like ErrWrongLeader would just
// hang instead of surfacing the mistake.
func (ck *CtrlerClerk) Move(shard, gid int) {
	ck.seqNum++
	args := &MoveArgs{Shard: shard, GID: gid, ClientID: ck.clientID, SeqNum: ck.seqNum}
	for {
		for i := 0; i < len(ck.servers); i++ {
			idx := (ck.lastKnown + i) % len(ck.servers)
			var reply MoveReply
			if err := ck.servers[idx].Move(args, &reply); err != nil {
				continue
			}
			switch reply.Err {
			case CtrlerOK:
				ck.lastKnown = idx
				return
			case CtrlerErrInvalidArgs:
				panic("shardkv: Move rejected as invalid (bad shard number or unknown group id)")
			}
			// ErrWrongLeader or ErrTimeout: try the next server, same args.
		}
		time.Sleep(raft.HeartbeatInterval)
	}
}

// Query returns the config at version num, or the latest if num is negative
// or beyond the latest known version.
func (ck *CtrlerClerk) Query(num int) Config {
	args := &QueryArgs{Num: num}
	for {
		for i := 0; i < len(ck.servers); i++ {
			idx := (ck.lastKnown + i) % len(ck.servers)
			var reply QueryReply
			if err := ck.servers[idx].Query(args, &reply); err != nil {
				continue
			}
			if reply.Err == CtrlerOK {
				ck.lastKnown = idx
				return reply.Config
			}
		}
		time.Sleep(raft.HeartbeatInterval)
	}
}
