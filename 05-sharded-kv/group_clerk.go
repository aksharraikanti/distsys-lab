package shardkv

import (
	"crypto/rand"
	"math/big"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
)

// ShardClerk is a client that routes through the controller instead of a
// config fixed at construction (unlike Day 1's ShardedClient, which is
// handed one Config and never learns of a change). It is the other half of
// TASKS.md's "the client refetches the config and retries": on
// GroupErrWrongGroup from whichever group its CACHED config says owns a
// key, it re-Querys the controller for the latest config and tries again.
//
// Like ShardedClient, servers are addressed in-process (a group id maps to
// its replica set directly, not through a pool/network client) — the same
// scoping call CtrlerClerk's own doc comment explains: proving the
// ownership-check mechanism is this day's job, real net/rpc serving for
// GroupServer is a later one's.
type ShardClerk struct {
	ctrl   *CtrlerClerk
	groups map[int][]*GroupServer // group id -> its replicas, in-process

	clientID int64
	seqNum   int64

	cfg Config // cached; refreshed only on GroupErrWrongGroup, not per call
}

// NewShardClerk returns a ShardClerk that starts from the controller's
// current config and will re-fetch it whenever a group says it's wrong.
func NewShardClerk(ctrl *CtrlerClerk, groups map[int][]*GroupServer) *ShardClerk {
	n, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		panic("shardkv: failed to generate ClientID: " + err.Error())
	}
	return &ShardClerk{
		ctrl:     ctrl,
		groups:   groups,
		clientID: n.Int64(),
		cfg:      ctrl.Query(-1),
	}
}

// Get fetches the current value for key, or "" if it has never been set.
func (ck *ShardClerk) Get(key string) string {
	for {
		gid := ck.cfg.Shards[Key2Shard(key)]
		wrongGroup := false
		for _, srv := range ck.groups[gid] {
			var reply GroupGetReply
			if err := srv.Get(&GroupGetArgs{Key: key}, &reply); err != nil {
				continue
			}
			switch reply.Err {
			case GroupOK:
				return reply.Value
			case GroupErrNoKey:
				return ""
			case GroupErrWrongGroup:
				wrongGroup = true
			}
			// GroupErrWrongLeader: try the next replica of the same group.
		}
		if wrongGroup {
			ck.cfg = ck.ctrl.Query(-1)
			continue
		}
		time.Sleep(raft.HeartbeatInterval)
	}
}

// Put sets key to value.
func (ck *ShardClerk) Put(key, value string) { ck.putAppend(key, value, "Put") }

// Append appends value onto whatever key currently holds.
func (ck *ShardClerk) Append(key, value string) { ck.putAppend(key, value, "Append") }

func (ck *ShardClerk) putAppend(key, value, op string) {
	ck.seqNum++
	for {
		gid := ck.cfg.Shards[Key2Shard(key)]
		args := &GroupPutAppendArgs{Key: key, Value: value, Op: op, ClientID: ck.clientID, SeqNum: ck.seqNum}
		wrongGroup := false
		for _, srv := range ck.groups[gid] {
			var reply GroupPutAppendReply
			if err := srv.PutAppend(args, &reply); err != nil {
				continue
			}
			switch reply.Err {
			case GroupOK:
				return
			case GroupErrWrongGroup:
				wrongGroup = true
			}
			// GroupErrWrongLeader or GroupErrTimeout: try the next replica.
		}
		if wrongGroup {
			ck.cfg = ck.ctrl.Query(-1)
			continue
		}
		time.Sleep(raft.HeartbeatInterval)
	}
}
