package hotshard

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	shardkv "github.com/aksharraikanti/distsys-lab/05-sharded-kv"
)

// RingAPI is the slice of the controller the auto-splitter needs. Keeping
// it an interface means the splitter's decision logic is testable against
// a fake, and a real RPC clerk can replace RingClerk without touching it.
type RingAPI interface {
	Query() (RingConfig, error)
	Split(shard ShardID, point uint32) error
	Move(shard ShardID, gid int) error
}

// RingClerk drives an in-process set of RingCtrler replicas, retrying
// across them until one that is leader answers — the same loop Stage 5's
// CtrlerClerk runs over RPC. Each logical request keeps one SeqNum across
// all its retries, which is what the controller's dedup relies on.
type RingClerk struct {
	ctrlers  []*RingCtrler
	clientID int64
	timeout  time.Duration

	mu  sync.Mutex
	seq int64
}

func NewRingClerk(ctrlers []*RingCtrler) *RingClerk {
	return &RingClerk{ctrlers: ctrlers, clientID: rand.Int63() + 1, timeout: 10 * time.Second}
}

func (ck *RingClerk) nextSeq() int64 {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.seq++
	return ck.seq
}

func (ck *RingClerk) retry(call func(c *RingCtrler) shardkv.CtrlerErr) error {
	deadline := time.Now().Add(ck.timeout)
	for time.Now().Before(deadline) {
		for _, c := range ck.ctrlers {
			switch err := call(c); err {
			case shardkv.CtrlerOK:
				return nil
			case shardkv.CtrlerErrWrongLeader, shardkv.CtrlerErrTimeout:
			default:
				return fmt.Errorf("hotshard: controller rejected request: %s", err)
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fmt.Errorf("hotshard: no controller leader answered within %s", ck.timeout)
}

func (ck *RingClerk) Query() (RingConfig, error) {
	var cfg RingConfig
	err := ck.retry(func(c *RingCtrler) shardkv.CtrlerErr {
		var r QueryReply
		c.Query(&QueryArgs{Num: -1}, &r)
		cfg = r.Config
		return r.Err
	})
	return cfg, err
}

func (ck *RingClerk) Split(shard ShardID, point uint32) error {
	seq := ck.nextSeq()
	return ck.retry(func(c *RingCtrler) shardkv.CtrlerErr {
		var r SplitReply
		c.Split(&SplitArgs{Shard: shard, Point: point, ClientID: ck.clientID, SeqNum: seq}, &r)
		return r.Err
	})
}

func (ck *RingClerk) Move(shard ShardID, gid int) error {
	seq := ck.nextSeq()
	return ck.retry(func(c *RingCtrler) shardkv.CtrlerErr {
		var r MoveReply
		c.Move(&MoveArgs{Shard: shard, GID: gid, ClientID: ck.clientID, SeqNum: seq}, &r)
		return r.Err
	})
}
