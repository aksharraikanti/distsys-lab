package hotshard

import (
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
	shardkv "github.com/aksharraikanti/distsys-lab/05-sharded-kv"
)

const (
	ringCommitTimeout       = 20 * raft.ElectionTimeoutMax
	ringLeaderCheckInterval = raft.HeartbeatInterval
)

// ringOp is the command that flows through the RingCtrler's Raft log. One
// struct with a Type tag and whichever fields that type uses, same shape as
// Stage 5's ctrlerOp.
type ringOp struct {
	Type string // "Init", "Split", "Move", or "Noop"

	InitRing   Ring             // Init
	InitGroups map[int][]string // Init
	Shard      ShardID          // Split, Move
	SplitPoint uint32           // Split
	MoveGID    int              // Move

	ClientID int64
	SeqNum   int64
}

func init() { gob.Register(ringOp{}) }

// applyResult is what applyLoop hands a waiting proposer: which request it
// applied and whether it actually took effect. Stage 5's Ctrler could only
// check Move's arguments BEFORE proposing and documented the gap that left
// (a group leaving between check and commit); a Split can't tolerate that
// gap — a stale split point silently dropped is a lost split — so every
// op here is validated at APPLY time, deterministically, on every replica,
// and the verdict travels back through the notify channel.
type applyResult struct {
	clientID int64
	seqNum   int64
	err      shardkv.CtrlerErr
}

type dedupEntry struct {
	seq int64
	err shardkv.CtrlerErr // the verdict of that seq, so a retry gets the same answer
}

// RingCtrler is Stage 5's Ctrler rebuilt on RingConfig: a Raft-replicated
// history of ring configurations, with Split as a first-class operation.
// Everything about replication — apply loop, dedup, leader no-op before
// reads — is Ctrler's machinery unchanged; only the state machine differs.
// Join/Leave aren't carried over: Stage 5's rebalance is defined over a
// fixed shard array, and rebalancing a dynamic ring is Day 5's Move-driven
// job, not something to re-derive here.
type RingCtrler struct {
	mu      sync.Mutex
	rf      *raft.Raft
	configs []RingConfig // configs[0] is the zero config: Num 0, nothing in it.

	notifyChans     map[int]chan applyResult
	duplicateTable  map[int64]dedupEntry
	noopAppliedTerm int

	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewRingCtrler(rf *raft.Raft) *RingCtrler {
	c := &RingCtrler{
		rf:             rf,
		configs:        []RingConfig{{}},
		notifyChans:    make(map[int]chan applyResult),
		duplicateTable: make(map[int64]dedupEntry),
		stopCh:         make(chan struct{}),
	}
	go c.applyLoop()
	go c.noopLoop()
	return c
}

func (c *RingCtrler) Stop() { c.stopOnce.Do(func() { close(c.stopCh) }) }

func (c *RingCtrler) latestLocked() RingConfig { return c.configs[len(c.configs)-1] }

// applyOp computes op's effect on the current config. Pure on c.configs'
// contents, so every replica reaches the same verdict.
func (c *RingCtrler) applyOp(op ringOp) shardkv.CtrlerErr {
	cur := c.latestLocked()
	var next RingConfig
	var err error
	switch op.Type {
	case "Init":
		if cur.Num != 0 {
			return shardkv.CtrlerErrInvalidArgs // already initialized
		}
		next, err = NewRingConfig(op.InitRing, op.InitGroups)
	case "Split":
		next, _, err = SplitConfig(cur, op.Shard, op.SplitPoint)
	case "Move":
		next, err = MoveRingShard(cur, op.Shard, op.MoveGID)
	case "Noop":
		return shardkv.CtrlerOK
	default:
		panic(fmt.Sprintf("hotshard: ring ctrler got unknown op type %q", op.Type))
	}
	if err != nil {
		return shardkv.CtrlerErrInvalidArgs
	}
	c.configs = append(c.configs, next)
	return shardkv.CtrlerOK
}

func (c *RingCtrler) applyLoop() {
	for {
		var msg raft.ApplyMsg
		select {
		case <-c.stopCh:
			return
		case msg = <-c.rf.ApplyCh:
		}
		op, ok := msg.Command.(ringOp)
		if !ok {
			panic(fmt.Sprintf("hotshard: ring ctrler applyLoop got a non-ringOp command at index %d: %#v", msg.Index, msg.Command))
		}

		c.mu.Lock()
		if op.Type == "Noop" {
			c.noopAppliedTerm = msg.Term
		}
		res := applyResult{clientID: op.ClientID, seqNum: op.SeqNum}
		if d := c.duplicateTable[op.ClientID]; op.Type != "Noop" && op.SeqNum <= d.seq {
			res.err = d.err
		} else {
			res.err = c.applyOp(op)
			if op.Type != "Noop" {
				c.duplicateTable[op.ClientID] = dedupEntry{seq: op.SeqNum, err: res.err}
			}
		}
		if ch, waiting := c.notifyChans[msg.Index]; waiting {
			delete(c.notifyChans, msg.Index)
			ch <- res
		}
		c.mu.Unlock()
	}
}

func (c *RingCtrler) noopLoop() {
	ticker := time.NewTicker(ringLeaderCheckInterval)
	defer ticker.Stop()
	lastNoopTerm := -1
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			if c.rf.State() != raft.Leader {
				continue
			}
			term := c.rf.Term()
			if term == lastNoopTerm {
				continue
			}
			if _, _, isLeader := c.rf.Propose(ringOp{Type: "Noop"}); isLeader {
				lastNoopTerm = term
			}
		}
	}
}

func (c *RingCtrler) proposeAndWait(op ringOp) shardkv.CtrlerErr {
	index, term, isLeader := c.rf.Propose(op)
	if !isLeader {
		return shardkv.CtrlerErrWrongLeader
	}
	c.mu.Lock()
	ch := make(chan applyResult, 1)
	c.notifyChans[index] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.notifyChans, index)
		c.mu.Unlock()
	}()

	leaderCheck := time.NewTicker(ringLeaderCheckInterval)
	defer leaderCheck.Stop()
	deadline := time.After(ringCommitTimeout)
	for {
		select {
		case applied := <-ch:
			if applied.clientID != op.ClientID || applied.seqNum != op.SeqNum {
				return shardkv.CtrlerErrWrongLeader
			}
			return applied.err
		case <-deadline:
			return shardkv.CtrlerErrTimeout
		case <-leaderCheck.C:
			if c.rf.Term() != term || c.rf.State() != raft.Leader {
				return shardkv.CtrlerErrWrongLeader
			}
		}
	}
}

type InitArgs struct {
	Ring     Ring
	Groups   map[int][]string
	ClientID int64
	SeqNum   int64
}
type InitReply struct{ Err shardkv.CtrlerErr }

// Init installs version 1 from args.Ring and args.Groups. Only valid on an
// empty history.
func (c *RingCtrler) Init(args *InitArgs, reply *InitReply) error {
	reply.Err = c.proposeAndWait(ringOp{Type: "Init", InitRing: args.Ring, InitGroups: args.Groups,
		ClientID: args.ClientID, SeqNum: args.SeqNum})
	return nil
}

type SplitArgs struct {
	Shard    ShardID
	Point    uint32
	ClientID int64
	SeqNum   int64
}
type SplitReply struct{ Err shardkv.CtrlerErr }

// Split proposes dividing args.Shard at args.Point as a new config version,
// both halves staying with the current owner. CtrlerErrInvalidArgs means it
// committed but didn't apply: the shard doesn't exist, or the point isn't
// strictly inside its range under the config the entry landed against
// (e.g. someone else split it first).
func (c *RingCtrler) Split(args *SplitArgs, reply *SplitReply) error {
	reply.Err = c.proposeAndWait(ringOp{Type: "Split", Shard: args.Shard, SplitPoint: args.Point,
		ClientID: args.ClientID, SeqNum: args.SeqNum})
	return nil
}

type MoveArgs struct {
	Shard    ShardID
	GID      int
	ClientID int64
	SeqNum   int64
}
type MoveReply struct{ Err shardkv.CtrlerErr }

// Move reassigns one shard to args.GID, checked at apply time.
func (c *RingCtrler) Move(args *MoveArgs, reply *MoveReply) error {
	reply.Err = c.proposeAndWait(ringOp{Type: "Move", Shard: args.Shard, MoveGID: args.GID,
		ClientID: args.ClientID, SeqNum: args.SeqNum})
	return nil
}

type QueryArgs struct{ Num int }
type QueryReply struct {
	Err    shardkv.CtrlerErr
	Config RingConfig
}

// Query is Ctrler.Query: leader-only, and only once this term's own no-op
// has applied (Raft §8). Num < 0 or past the latest means the latest.
func (c *RingCtrler) Query(args *QueryArgs, reply *QueryReply) error {
	if c.rf.State() != raft.Leader {
		reply.Err = shardkv.CtrlerErrWrongLeader
		return nil
	}
	term := c.rf.Term()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.noopAppliedTerm != term {
		reply.Err = shardkv.CtrlerErrWrongLeader
		return nil
	}
	idx := args.Num
	if idx < 0 || idx >= len(c.configs) {
		idx = len(c.configs) - 1
	}
	reply.Config = c.configs[idx]
	reply.Err = shardkv.CtrlerOK
	return nil
}
