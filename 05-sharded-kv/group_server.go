package shardkv

import (
	"fmt"
	"sync"
	"time"

	raft "github.com/aksharraikanti/distsys-lab/01-raft"
)

// groupCommitTimeout/groupLeaderCheckInterval/groupConfigPollInterval are
// 02-kv-store's own commitTimeout/leaderCheckInterval, plus one new tunable
// for this stage: how often a group's leader checks the controller for a
// newer config. Reusing raft.HeartbeatInterval for the poll, rather than
// inventing a fourth timing constant, keeps every background loop in this
// project on the same handful of derived intervals (see 03-connection-
// pooling's own reasoning for doing the same with its pool timeouts).
const (
	groupCommitTimeout       = 20 * raft.ElectionTimeoutMax
	groupLeaderCheckInterval = raft.HeartbeatInterval
	groupConfigPollInterval  = raft.HeartbeatInterval
)

// groupApplyResult is what applyLoop hands back to a waiting PutAppend
// through the notify channel: which op actually landed at that index (so
// the waiter can detect it was superseded by someone else's), and — new for
// this stage — the outcome the state machine decided AT APPLY TIME, which
// for Put/Append can be GroupErrWrongGroup even though the entry itself
// committed successfully. A KVServer never had this distinction: applying
// an entry and it succeeding were the same event. Here, an entry can commit
// and still be the wrong group's problem, once this replica's own view of
// which config is current has moved on since the client proposed it.
type groupApplyResult struct {
	op  groupOp
	err GroupErr
}

// GroupServer is one shard GROUP's Raft-backed KV store: 02-kv-store's
// KVServer, but restricted to serving only the shards its group currently
// owns. It is a new type rather than an extension of KVServer for the same
// reason Ctrler is a new type rather than a KVServer subclass — the command
// type Put/Append/Noop needed extending (with Config) in a way KVServer's
// unexported, closed Op switch has no seam for, and forking the type is more
// honest than reaching into another stage's package internals.
//
// gid identifies which entry in a Config's Groups this server IS — the
// other half of the "am I the owner" question cfg.Shards[shard] answers.
// cfg is this replica's own view of the current config, and critically: the
// ONLY place it is ever written is applyLoop, from a committed Config entry
// — see applyLoop's doc comment for why that, and not a value written
// directly by configPollLoop, is what "checked through the log, not a local
// variable" (TASKS.md's own words for this day) actually means in code.
type GroupServer struct {
	mu    sync.Mutex
	rf    *raft.Raft
	gid   int
	ctrl  *CtrlerClerk
	cfg   Config
	store map[string]string

	notifyChans    map[int]chan groupApplyResult
	duplicateTable map[int64]int64

	noopAppliedTerm int

	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewGroupServer wraps rf as group gid's server, polling ctrl for new
// configs. Same split of responsibility as NewKVServer/NewCtrler: the
// caller starts rf's own background loops; NewGroupServer starts only this
// server's own consumers of them. Call Stop when done.
func NewGroupServer(rf *raft.Raft, gid int, ctrl *CtrlerClerk) *GroupServer {
	s := &GroupServer{
		rf:             rf,
		gid:            gid,
		ctrl:           ctrl,
		store:          make(map[string]string),
		notifyChans:    make(map[int]chan groupApplyResult),
		duplicateTable: make(map[int64]int64),
		stopCh:         make(chan struct{}),
	}
	go s.applyLoop()
	go s.noopLoop()
	go s.configPollLoop()
	return s
}

// Stop terminates this GroupServer's background goroutines. Safe to call
// more than once. Does not touch the underlying *raft.Raft.
func (s *GroupServer) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
}

// ownsLocked reports whether this group currently owns key's shard, per
// this replica's own applied Config. Caller must hold s.mu.
func (s *GroupServer) ownsLocked(key string) bool {
	return s.cfg.Shards[Key2Shard(key)] == s.gid
}

// applyLoop is 02-kv-store's applyLoop with a third command kind. Put/Append
// still check dedup and mutate store — but ONLY after checking ownsLocked,
// and that check reads s.cfg, which this very function is the sole writer
// of. That closure is what makes ownership "checked through the log": every
// replica applies the identical sequence of Config and Put/Append entries in
// the identical order, so every replica's s.cfg at the moment a given
// Put/Append applies is identical too, deterministically, with no window
// where two replicas (or this replica across an election) could disagree
// about which config was current for THAT specific entry.
//
// A Config entry is adopted only if op.Config.Num == s.cfg.Num+1 — anything
// else (a duplicate proposal from two nodes that both briefly believed
// themselves leader, or a stale retry from configPollLoop after this
// replica already advanced) is silently ignored, which is safe: Config.Num
// is a strict sequence, so "not exactly next" can only mean "already
// applied" or "out of order," never "a real update this replica hasn't
// seen yet."
func (s *GroupServer) applyLoop() {
	for {
		var msg raft.ApplyMsg
		select {
		case <-s.stopCh:
			return
		case msg = <-s.rf.ApplyCh:
		}

		op, ok := msg.Command.(groupOp)
		if !ok {
			panic(fmt.Sprintf("shardkv: group %d applyLoop received a non-groupOp command at index %d: %#v", s.gid, msg.Index, msg.Command))
		}

		s.mu.Lock()
		result := groupApplyResult{op: op}
		switch op.Type {
		case "Noop":
			s.noopAppliedTerm = msg.Term
			result.err = GroupOK
		case "Config":
			if op.Config.Num == s.cfg.Num+1 {
				s.cfg = op.Config
			}
			result.err = GroupOK
		case "Put", "Append":
			if !s.ownsLocked(op.Key) {
				result.err = GroupErrWrongGroup
			} else {
				if op.SeqNum > s.duplicateTable[op.ClientID] {
					switch op.Type {
					case "Put":
						s.store[op.Key] = op.Value
					case "Append":
						s.store[op.Key] += op.Value
					}
					s.duplicateTable[op.ClientID] = op.SeqNum
				}
				result.err = GroupOK
			}
		default:
			panic(fmt.Sprintf("shardkv: group %d applyLoop received an unknown op type %q at index %d", s.gid, op.Type, msg.Index))
		}
		if ch, waiting := s.notifyChans[msg.Index]; waiting {
			delete(s.notifyChans, msg.Index)
			ch <- result
		}
		s.mu.Unlock()
	}
}

// noopLoop is KVServer's noopLoop verbatim in spirit — see its doc comment.
// Get and the ownership check both need it: without a just-elected leader
// having applied its own-term no-op, it cannot trust ANYTHING in its state
// machine, cfg included, actually reflects everything the cluster already
// committed.
func (s *GroupServer) noopLoop() {
	ticker := time.NewTicker(groupLeaderCheckInterval)
	defer ticker.Stop()

	lastNoopTerm := -1
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			if s.rf.State() != raft.Leader {
				continue
			}
			term := s.rf.Term()
			if term == lastNoopTerm {
				continue
			}
			if _, _, isLeader := s.rf.Propose(groupOp{Type: "Noop"}); isLeader {
				lastNoopTerm = term
			}
		}
	}
}

// configPollLoop is this group's half of "each group polls the controller
// for the current config" (TASKS.md): once per tick, a leader asks for
// EXACTLY the next config version past what it has applied (never the
// absolute latest) — configs must be adopted one at a time, in order,
// because a real migration (Day 5) has to react to each individual
// transition, not just "where we ended up." lastProposed avoids re-log-ging
// the same pending Config entry every tick while it's still waiting to
// commit; it's reset the moment this node is no longer observed as leader,
// which matters for real correctness, not just log tidiness — without the
// reset, a node that proposed Num N as leader, lost leadership before it
// committed, and later became leader again at the SAME s.cfg.Num would
// believe "already proposed, just waiting" forever and never re-propose,
// even if that original entry never actually made it into the durable log.
func (s *GroupServer) configPollLoop() {
	ticker := time.NewTicker(groupConfigPollInterval)
	defer ticker.Stop()

	lastProposed := -1
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			if s.rf.State() != raft.Leader {
				lastProposed = -1
				continue
			}
			s.mu.Lock()
			nextNum := s.cfg.Num + 1
			s.mu.Unlock()
			if nextNum == lastProposed {
				continue
			}
			next := s.ctrl.Query(nextNum)
			if next.Num != nextNum {
				continue // controller doesn't have this version yet
			}
			if _, _, isLeader := s.rf.Propose(groupOp{Type: "Config", Config: next}); isLeader {
				lastProposed = nextNum
			}
		}
	}
}

// proposeAndWait is Ctrler's own proposeAndWait, adapted to groupOp/
// groupApplyResult: the supersession check still compares ClientID/SeqNum
// rather than the whole op (groupOp carries a Config field, not
// comparable), but the outcome on a genuine match is now whatever applyLoop
// decided (result.err), not always GroupOK — see groupApplyResult's doc
// comment for why that distinction exists here and didn't for Ctrler's own
// Join/Leave/Move.
func (s *GroupServer) proposeAndWait(op groupOp) GroupErr {
	index, term, isLeader := s.rf.Propose(op)
	if !isLeader {
		return GroupErrWrongLeader
	}

	s.mu.Lock()
	ch := make(chan groupApplyResult, 1)
	s.notifyChans[index] = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.notifyChans, index)
		s.mu.Unlock()
	}()

	leaderCheck := time.NewTicker(groupLeaderCheckInterval)
	defer leaderCheck.Stop()
	deadline := time.After(groupCommitTimeout)

	for {
		select {
		case applied := <-ch:
			if applied.op.ClientID != op.ClientID || applied.op.SeqNum != op.SeqNum {
				return GroupErrWrongLeader
			}
			return applied.err
		case <-deadline:
			return GroupErrTimeout
		case <-leaderCheck.C:
			if s.rf.Term() != term || s.rf.State() != raft.Leader {
				return GroupErrWrongLeader
			}
		}
	}
}

// Get is KVServer.Get's own leader-and-noop-applied gate, plus the
// ownership check — both read under the same lock, from the same
// applyLoop-written state, so they see a single consistent snapshot: no
// window where the noop check passes against one cfg and the ownership
// check runs against a newer one.
func (s *GroupServer) Get(args *GroupGetArgs, reply *GroupGetReply) error {
	if s.rf.State() != raft.Leader {
		reply.Err = GroupErrWrongLeader
		return nil
	}
	term := s.rf.Term()
	s.mu.Lock()
	if s.noopAppliedTerm != term {
		s.mu.Unlock()
		reply.Err = GroupErrWrongLeader
		return nil
	}
	if !s.ownsLocked(args.Key) {
		s.mu.Unlock()
		reply.Err = GroupErrWrongGroup
		return nil
	}
	value, ok := s.store[args.Key]
	s.mu.Unlock()
	if !ok {
		reply.Err = GroupErrNoKey
		return nil
	}
	reply.Value = value
	reply.Err = GroupOK
	return nil
}

// PutAppend proposes the write and waits for it to resolve. The ownership
// pre-check here is a pure optimization — it saves an obviously-doomed
// request from ever entering the Raft log — NOT the authoritative decision;
// that's applyLoop's job, against whatever s.cfg turns out to be current
// once this specific entry actually applies. A shard could change hands in
// the gap between this pre-check and commit, and applyLoop's check is what
// catches that, not this one.
func (s *GroupServer) PutAppend(args *GroupPutAppendArgs, reply *GroupPutAppendReply) error {
	s.mu.Lock()
	preOwned := s.ownsLocked(args.Key)
	s.mu.Unlock()
	if !preOwned {
		reply.Err = GroupErrWrongGroup
		return nil
	}

	op := groupOp{Type: args.Op, Key: args.Key, Value: args.Value, ClientID: args.ClientID, SeqNum: args.SeqNum}
	reply.Err = s.proposeAndWait(op)
	return nil
}
