package shardkv

import (
	"fmt"
	"sync"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
)

// ctrlerCommitTimeout/ctrlerLeaderCheckInterval are 02-kv-store's own
// commitTimeout/leaderCheckInterval, renamed rather than reused: this file
// lives in package shardkv, not kvstore, so there's no name collision to
// avoid, but a name like "commitTimeout" reused for an entirely different
// server type in a different package would read as more shared than it
// actually is. Same values, same reasoning — see 02-kv-store/server.go's
// doc comment on the originals for why these two particular constants.
const (
	ctrlerCommitTimeout       = 20 * raft.ElectionTimeoutMax
	ctrlerLeaderCheckInterval = raft.HeartbeatInterval
)

// Ctrler is the shard controller: a Raft-replicated log of Config versions.
// It is "Stage 2 again in a new costume" (TASKS.md's own words) — the same
// apply-loop, dedup-table, notify-channel, and no-op-on-election machinery
// as KVServer, driving a slice of Configs instead of a map[string]string.
//
// Unlike KVServer, Ctrler has no snapshotting: a config history grows one
// entry per Join/Leave/Move, which in any realistic run is a handful to a
// few thousand versions — orders of magnitude smaller than a KV store's
// state, and nowhere near worth the complexity Stage 2 didn't need until
// its Day 6. If that assumption ever stops holding, the exact same
// kvSnapshot/RaftStateSize pattern KVServer uses would carry over directly.
type Ctrler struct {
	mu      sync.Mutex
	rf      *raft.Raft
	configs []Config // configs[0] is the zero Config: Num 0, no groups.

	// notifyChans/duplicateTable are exactly KVServer's fields, renamed for
	// this type's own command (ctrlerOp instead of Op) — see
	// 02-kv-store/server.go's doc comments for the full reasoning; it
	// applies here unchanged.
	notifyChans    map[int]chan ctrlerOp
	duplicateTable map[int64]int64

	// noopAppliedTerm is KVServer's own field and rule, applied to Query
	// instead of Get: a freshly elected leader must not answer Query until
	// it has applied a no-op from its own term (Raft §8), or it could
	// silently omit a config version the previous leader already
	// acknowledged as committed.
	noopAppliedTerm int

	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewCtrler wraps rf and starts serving Join/Leave/Move/Query once rf's own
// background loops (RunElectionTimer, RunHeartbeats, RunApplyLoop — the
// caller's responsibility, same split as NewKVServer) are running. Call
// Stop when done to shut down this Ctrler's own goroutines.
func NewCtrler(rf *raft.Raft) *Ctrler {
	c := &Ctrler{
		rf:             rf,
		configs:        []Config{{}},
		notifyChans:    make(map[int]chan ctrlerOp),
		duplicateTable: make(map[int64]int64),
		stopCh:         make(chan struct{}),
	}
	go c.applyLoop()
	go c.noopLoop()
	return c
}

// Stop terminates this Ctrler's background goroutines. Safe to call more
// than once. Does not touch the underlying *raft.Raft.
func (c *Ctrler) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
	})
}

// latestLocked returns the current (highest-Num) config. Caller must hold
// c.mu.
func (c *Ctrler) latestLocked() Config {
	return c.configs[len(c.configs)-1]
}

// applyLoop is applyLoop from 02-kv-store/server.go with the state machine
// swapped: instead of mutating a map[string]string, each committed
// Join/Leave/Move APPENDS a new Config computed by config.go's own pure
// functions from the current latest one. Dedup (op.SeqNum >
// duplicateTable[op.ClientID]) guards this exactly the way it guards
// KVServer's store mutations — without it, a retried Join reaching a second
// log index would append a SECOND new config version for the same logical
// request, corrupting the Num sequence into something that no longer lines
// up 1:1 with "how many real changes happened."
func (c *Ctrler) applyLoop() {
	for {
		var msg raft.ApplyMsg
		select {
		case <-c.stopCh:
			return
		case msg = <-c.rf.ApplyCh:
		}

		op, ok := msg.Command.(ctrlerOp)
		if !ok {
			panic(fmt.Sprintf("shardkv: ctrler applyLoop received a non-ctrlerOp command at index %d: %#v", msg.Index, msg.Command))
		}

		c.mu.Lock()
		if op.Type == "Noop" {
			c.noopAppliedTerm = msg.Term
		}
		if op.SeqNum > c.duplicateTable[op.ClientID] {
			switch op.Type {
			case "Join":
				c.configs = append(c.configs, Join(c.latestLocked(), op.JoinGroups))
			case "Leave":
				c.configs = append(c.configs, Leave(c.latestLocked(), op.LeaveGIDs))
			case "Move":
				c.configs = append(c.configs, Move(c.latestLocked(), op.MoveShard, op.MoveGID))
			case "Noop":
				// Deliberately no config mutation — see noopLoop's doc
				// comment for why this op exists at all.
			default:
				panic(fmt.Sprintf("shardkv: ctrler applyLoop received an unknown op type %q at index %d", op.Type, msg.Index))
			}
			c.duplicateTable[op.ClientID] = op.SeqNum
		}
		if ch, waiting := c.notifyChans[msg.Index]; waiting {
			delete(c.notifyChans, msg.Index)
			ch <- op
		}
		c.mu.Unlock()
	}
}

// noopLoop is 02-kv-store's noopLoop verbatim in spirit: the moment this
// node observes itself leading a new term, it Proposes a no-op so Query can
// eventually be answered safely (Raft §8) — see KVServer.noopLoop's doc
// comment for the full reasoning, which applies here without change. Reuses
// ClientID 0 as the reserved no-op sentinel, same as Stage 2.
func (c *Ctrler) noopLoop() {
	ticker := time.NewTicker(ctrlerLeaderCheckInterval)
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
			if _, _, isLeader := c.rf.Propose(ctrlerOp{Type: "Noop"}); isLeader {
				lastNoopTerm = term
			}
		}
	}
}

// proposeAndWait is the waiting logic PutAppend uses, factored out since
// Join/Leave/Move share it identically and differ only in which ctrlerOp
// they build. One real difference from PutAppend's own version: it compares
// applied.ClientID/SeqNum, not the whole applied op, to detect whether a
// DIFFERENT request landed at this index instead of this one. PutAppend can
// compare its whole Op because kvstore.Op's fields are all comparable
// (strings and ints); ctrlerOp carries a map and a slice (JoinGroups,
// LeaveGIDs), which Go does not let you compare with ==. ClientID+SeqNum
// already uniquely identifies "this specific proposal" on its own — a
// narrower, and arguably more direct, statement of the same check.
func (c *Ctrler) proposeAndWait(op ctrlerOp) CtrlerErr {
	index, term, isLeader := c.rf.Propose(op)
	if !isLeader {
		return CtrlerErrWrongLeader
	}

	c.mu.Lock()
	ch := make(chan ctrlerOp, 1)
	c.notifyChans[index] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.notifyChans, index)
		c.mu.Unlock()
	}()

	leaderCheck := time.NewTicker(ctrlerLeaderCheckInterval)
	defer leaderCheck.Stop()
	deadline := time.After(ctrlerCommitTimeout)

	for {
		select {
		case applied := <-ch:
			if applied.ClientID != op.ClientID || applied.SeqNum != op.SeqNum {
				return CtrlerErrWrongLeader
			}
			return CtrlerOK
		case <-deadline:
			return CtrlerErrTimeout
		case <-leaderCheck.C:
			if c.rf.Term() != term || c.rf.State() != raft.Leader {
				return CtrlerErrWrongLeader
			}
		}
	}
}

// Join adds (or, for an already-known gid, updates the addresses of) each
// group in args.Groups and rebalances shards across the result.
func (c *Ctrler) Join(args *JoinArgs, reply *JoinReply) error {
	op := ctrlerOp{Type: "Join", JoinGroups: args.Groups, ClientID: args.ClientID, SeqNum: args.SeqNum}
	reply.Err = c.proposeAndWait(op)
	return nil
}

// Leave removes each group in args.GIDs and rebalances shards across
// whatever remains.
func (c *Ctrler) Leave(args *LeaveArgs, reply *LeaveReply) error {
	op := ctrlerOp{Type: "Leave", LeaveGIDs: args.GIDs, ClientID: args.ClientID, SeqNum: args.SeqNum}
	reply.Err = c.proposeAndWait(op)
	return nil
}

// Move reassigns exactly one shard to args.GID, rebalancing nothing else.
// Rejected before ever being proposed (CtrlerErrInvalidArgs) if the shard
// number is out of range or GID isn't a group in the current config — see
// CtrlerErrInvalidArgs's doc comment for the narrow race this check does
// NOT close.
func (c *Ctrler) Move(args *MoveArgs, reply *MoveReply) error {
	if args.Shard < 0 || args.Shard >= NShards {
		reply.Err = CtrlerErrInvalidArgs
		return nil
	}
	c.mu.Lock()
	_, knownGroup := c.latestLocked().Groups[args.GID]
	c.mu.Unlock()
	if !knownGroup {
		reply.Err = CtrlerErrInvalidArgs
		return nil
	}
	op := ctrlerOp{Type: "Move", MoveShard: args.Shard, MoveGID: args.GID, ClientID: args.ClientID, SeqNum: args.SeqNum}
	reply.Err = c.proposeAndWait(op)
	return nil
}

// Query returns the config at version args.Num, or the latest if args.Num
// is negative or beyond the latest known version. It is KVServer.Get's own
// leader-and-noop-applied gate (see its doc comment), applied to a read of
// c.configs instead of c.store: Query never goes through Raft itself, the
// same way Get never logs a Get.
func (c *Ctrler) Query(args *QueryArgs, reply *QueryReply) error {
	if c.rf.State() != raft.Leader {
		reply.Err = CtrlerErrWrongLeader
		return nil
	}
	// Term read before noopAppliedTerm for the same reason Get does this:
	// if leadership changes in between, the stale term can only read as
	// LOWER than whatever's applied, which the State() check above having
	// just passed already rules out for a node still leading that term.
	term := c.rf.Term()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.noopAppliedTerm != term {
		reply.Err = CtrlerErrWrongLeader
		return nil
	}
	idx := args.Num
	if idx < 0 || idx >= len(c.configs) {
		idx = len(c.configs) - 1
	}
	reply.Config = c.configs[idx]
	reply.Err = CtrlerOK
	return nil
}
