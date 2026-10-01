package shardkv

import (
	"bytes"
	"encoding/gob"
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
	groupCommitTimeout         = 20 * raft.ElectionTimeoutMax
	groupLeaderCheckInterval   = raft.HeartbeatInterval
	groupConfigPollInterval    = raft.HeartbeatInterval
	groupMigrationPollInterval = raft.HeartbeatInterval
	groupGCPollInterval        = raft.HeartbeatInterval
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

	// migrating[shard] is the group id this replica pulled (or still needs
	// to pull) shard from, present only for a shard this group has gained
	// ownership of but not yet received data for. A shard counts as ready
	// to serve only once it's both in cfg.Shards (owned) AND absent from
	// migrating — see ownsLocked. Written only by applyLoop, same as cfg:
	// a Config entry that grants a NEW shard adds an entry; a Migrate entry
	// that successfully lands the pulled data removes it.
	migrating map[int]int

	// leaving[shard] is the group id this replica gave shard away TO,
	// present only for a shard this group used to own but doesn't anymore,
	// that hasn't been garbage-collected yet — migrating's mirror image.
	// gcLoop confirms the new owner actually has it (HasShard) before
	// Proposing a GC entry that deletes the shard's data and clears this
	// entry. Without it, Day 5's migration leaks every shard it ever gives
	// away forever (TASKS.md's own "challenge" framing for this day).
	leaving map[int]int

	// peers resolves another group's id to its own replica set, for
	// migrationLoop's Pull calls. In-process, the same scoping choice (and
	// the same reason) CtrlerClerk/ShardClerk already made: proving the
	// migration PROTOCOL is correct doesn't need real sockets between
	// processes, only real Raft logs within each one. A real deployment
	// would dial the addresses a Config's own Groups map already carries
	// instead — set via SetPeers since a group's peers aren't all known
	// before every group in a test has been constructed.
	peers map[int][]*GroupServer

	notifyChans    map[int]chan groupApplyResult
	duplicateTable map[int64]int64

	noopAppliedTerm int

	// maxRaftState bounds how large rf's persisted state is allowed to grow
	// before applyLoop snapshots — KVServer's own field and reasoning (see
	// its doc comment), unused by any day before this one since nothing
	// here needed log compaction until a day actually combined long-running
	// traffic, reconfiguration, and faults in one scenario. -1 disables it.
	maxRaftState int

	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewGroupServer wraps rf as group gid's server, polling ctrl for new
// configs. Same split of responsibility as NewKVServer/NewCtrler: the
// caller starts rf's own background loops; NewGroupServer starts only this
// server's own consumers of them. Call Stop when done.
//
// maxRaftState is the size threshold (bytes of rf's persisted state) that
// triggers a snapshot — see the field's own doc comment. Pass -1 to disable
// snapshotting, which every day before this one did implicitly by never
// having the field at all. If rf already has a snapshot persisted (this
// node is restarting, not booting fresh), its state is restored before any
// loop starts — same ordering KVServer's own constructor uses, and for the
// same reason: those loops are the first things that could observe
// (or mutate) this server's state once they're running.
func NewGroupServer(rf *raft.Raft, gid int, ctrl *CtrlerClerk, maxRaftState int) *GroupServer {
	s := &GroupServer{
		rf:             rf,
		gid:            gid,
		ctrl:           ctrl,
		store:          make(map[string]string),
		migrating:      make(map[int]int),
		leaving:        make(map[int]int),
		notifyChans:    make(map[int]chan groupApplyResult),
		duplicateTable: make(map[int64]int64),
		maxRaftState:   maxRaftState,
		stopCh:         make(chan struct{}),
	}
	if data := rf.ReadSnapshot(); len(data) > 0 {
		s.restoreSnapshot(data)
	}
	go s.applyLoop()
	go s.noopLoop()
	go s.configPollLoop()
	go s.migrationLoop()
	go s.gcLoop()
	return s
}

// SetPeers gives this GroupServer a way to reach every other group it might
// ever need to pull a shard from. See the peers field's own doc comment for
// why this is a setter rather than a constructor argument: a test (or a
// real deployment resolving addresses lazily) can't always know the full
// set of groups before every one of them has been constructed.
func (s *GroupServer) SetPeers(peers map[int][]*GroupServer) {
	s.mu.Lock()
	s.peers = peers
	s.mu.Unlock()
}

// Stop terminates this GroupServer's background goroutines. Safe to call
// more than once. Does not touch the underlying *raft.Raft.
func (s *GroupServer) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
}

// ownsLocked reports whether this group is actually ready to serve key's
// shard: cfg says this group is the owner AND the shard isn't still waiting
// on a migration pull. Both halves matter — gaining a shard in cfg without
// its data would otherwise serve empty/wrong answers for keys that really
// do have a value, just not on this replica yet. Caller must hold s.mu.
func (s *GroupServer) ownsLocked(key string) bool {
	shard := Key2Shard(key)
	if s.cfg.Shards[shard] != s.gid {
		return false
	}
	_, stillMigrating := s.migrating[shard]
	return !stillMigrating
}

// groupSnapshot is everything a GroupServer needs to fully reconstruct its
// state without replaying a single log entry — KVServer's own kvSnapshot
// (Store, DuplicateTable), plus every piece of state this stage's own days
// added on top: Cfg (which config this replica has adopted — restoring
// store/duplicateTable without it would leave a replica with data but no
// idea which shards it's actually allowed to serve), and Migrating/Leaving
// (an in-flight migration or GC that was only HALF done as of the snapshot
// must resume exactly where it left off, not be silently forgotten — a
// forgotten `migrating` entry would mean this replica believes it owns a
// shard it never actually finished pulling, and a forgotten `leaving` entry
// would mean a shard never gets garbage-collected at all).
type groupSnapshot struct {
	Store          map[string]string
	DuplicateTable map[int64]int64
	Cfg            Config
	Migrating      map[int]int
	Leaving        map[int]int
}

// restoreSnapshot decodes data (as produced by snapshotLocked) and adopts
// it as this GroupServer's entire state — wholesale, not merged in. Called
// from two places, the same split KVServer's own restoreSnapshot is: from
// NewGroupServer before any loop starts (no lock needed — nothing else can
// be touching this server's state that early), and from applyLoop (Day 8,
// via msg.SnapshotValid) when this replica's own Raft node just installed a
// snapshot from its leader, under s.mu, since real client/background-loop
// traffic can be racing it by then.
func (s *GroupServer) restoreSnapshot(data []byte) {
	var snap groupSnapshot
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&snap); err != nil {
		panic(fmt.Sprintf("shardkv: group %d failed to decode persisted snapshot: %v", s.gid, err))
	}
	s.store = snap.Store
	s.duplicateTable = snap.DuplicateTable
	s.cfg = snap.Cfg
	s.migrating = snap.Migrating
	s.leaving = snap.Leaving
}

// snapshotLocked serializes the current state and hands it to Raft along
// with index — the log index this state reflects — so Raft can discard its
// own log through that point. Caller must hold s.mu.
func (s *GroupServer) snapshotLocked(index int) {
	buf := new(bytes.Buffer)
	snap := groupSnapshot{
		Store:          s.store,
		DuplicateTable: s.duplicateTable,
		Cfg:            s.cfg,
		Migrating:      s.migrating,
		Leaving:        s.leaving,
	}
	if err := gob.NewEncoder(buf).Encode(snap); err != nil {
		panic(fmt.Sprintf("shardkv: group %d failed to encode snapshot: %v", s.gid, err))
	}
	if err := s.rf.Snapshot(index, buf.Bytes()); err != nil {
		panic(fmt.Sprintf("shardkv: group %d failed to snapshot through index %d: %v", s.gid, index, err))
	}
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
//
// Adopting a Config that GAINS this group a shard it didn't own before adds
// an entry to migrating instead of serving it immediately — UNLESS the
// shard's previous owner was gid 0 (never really owned by anyone, e.g. the
// very first config a fresh deployment ever adopts), which has no data to
// pull and is ready the instant ownership is. The old owner (op.Config's
// predecessor, i.e. whatever s.cfg was a moment ago, captured before this
// line overwrites it) is recorded so migrationLoop knows who to Pull from —
// captured NOW because s.cfg is about to be overwritten, and nothing else
// remembers what it used to be. Symmetrically, a Config that COSTS this
// group a shard it used to own adds an entry to leaving, so gcLoop knows
// which group to ask "do you actually have it yet" before deleting.
//
// A Migrate entry lands one shard's pulled data: merge its key/value pairs
// into store, merge its dedup entries into duplicateTable by taking the
// MAX SeqNum per ClientID (never overwrite a higher one this replica may
// have already applied independently — the donor's table is a snapshot,
// not a fresher source of truth), then clear the migrating entry. Both
// merges are idempotent, so a duplicate or re-proposed Migrate for a shard
// that's already landed is silently a no-op, not an error.
//
// A GC entry drops one shard's data — every key whose Key2Shard matches —
// but ONLY if the shard is still in leaving; a GC entry that arrives after
// the shard has somehow already been cleared (a duplicate proposal, or this
// same shard coming back around through a LATER reassignment — see gcLoop's
// own doc comment) is a no-op, not a second deletion of who-knows-what.
// duplicateTable is deliberately untouched: it isn't scoped per shard (see
// Migrate's own reasoning), so there's no safe per-shard subset of it to
// remove without risking a regression on a DIFFERENT shard this group still
// owns.
func (s *GroupServer) applyLoop() {
	for {
		var msg raft.ApplyMsg
		select {
		case <-s.stopCh:
			return
		case msg = <-s.rf.ApplyCh:
		}

		if msg.SnapshotValid {
			s.mu.Lock()
			s.restoreSnapshot(msg.Snapshot)
			s.mu.Unlock()
			continue
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
				old := s.cfg
				s.cfg = op.Config
				for shard := 0; shard < NShards; shard++ {
					oldOwner, newOwner := old.Shards[shard], op.Config.Shards[shard]
					if newOwner == s.gid && oldOwner != s.gid && oldOwner != 0 {
						s.migrating[shard] = oldOwner
					}
					if oldOwner == s.gid && newOwner != s.gid && newOwner != 0 {
						s.leaving[shard] = newOwner
					}
				}
			}
			result.err = GroupOK
		case "Migrate":
			if _, stillMigrating := s.migrating[op.Shard]; stillMigrating {
				for k, v := range op.Data {
					s.store[k] = v
				}
				for clientID, seq := range op.DupTable {
					if seq > s.duplicateTable[clientID] {
						s.duplicateTable[clientID] = seq
					}
				}
				delete(s.migrating, op.Shard)
			}
			result.err = GroupOK
		case "GC":
			if _, stillLeaving := s.leaving[op.Shard]; stillLeaving {
				for k := range s.store {
					if Key2Shard(k) == op.Shard {
						delete(s.store, k)
					}
				}
				delete(s.leaving, op.Shard)
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
		// Checked after every applied entry, not on a separate timer — same
		// reasoning as KVServer's own version of this check: RaftStateSize
		// only grows one entry at a time, applyLoop already holds s.mu with
		// a consistent, just-applied view of every piece of state
		// snapshotLocked needs, and there's no way to overshoot the
		// threshold between checks.
		if s.maxRaftState != -1 && s.rf.RaftStateSize() >= s.maxRaftState {
			s.snapshotLocked(msg.Index)
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

// migrationLoop is this group's half of "the new owner PULLS the shards it
// gained from their previous owner" (TASKS.md): once per tick, a leader
// checks for any shard still in migrating, Pulls it from the recorded
// source group, and — once the pull actually returns data — Proposes a
// Migrate entry so every replica in THIS group lands the data identically,
// the same "through the log" reasoning applyLoop's own doc comment gives
// for Config. Fire-and-forget, not proposeAndWait: Migrate's effect is
// idempotent (see applyLoop), so if this Propose is lost to a leadership
// change before it commits, the next tick (by this node or whoever leads
// next) just re-pulls and re-proposes — no different from how
// configPollLoop handles the same failure mode for Config entries.
//
// proposed tracks, per shard, whether THIS node already has a Migrate entry
// outstanding for it, so a slow-committing proposal isn't re-sent every
// tick; reset whenever this node is no longer observed as leader, for the
// identical correctness reason configPollLoop's lastProposed is (see its
// own doc comment) — an entry proposed while leader that never actually
// committed must be retried once leadership is regained, not believed
// forever pending.
func (s *GroupServer) migrationLoop() {
	ticker := time.NewTicker(groupMigrationPollInterval)
	defer ticker.Stop()

	proposed := make(map[int]bool)
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			if s.rf.State() != raft.Leader {
				proposed = make(map[int]bool)
				continue
			}
			s.mu.Lock()
			pending := make(map[int]int, len(s.migrating))
			for shard, from := range s.migrating {
				pending[shard] = from
			}
			configNum := s.cfg.Num
			peers := s.peers
			s.mu.Unlock()

			for shard, fromGID := range pending {
				if proposed[shard] {
					continue
				}
				reply, ok := s.pullFrom(peers[fromGID], shard, configNum)
				if !ok {
					continue
				}
				if _, _, isLeader := s.rf.Propose(groupOp{Type: "Migrate", Shard: shard, Data: reply.Data, DupTable: reply.DupTable}); isLeader {
					proposed[shard] = true
				}
			}
		}
	}
}

// pullFrom tries each replica of a donor group in turn until one actually
// answers — the same round-robin-until-success shape every Clerk in this
// project uses — and reports ok=false (try again next tick) rather than
// blocking if none does or the donor says GroupErrNotReady. Migration must
// never block the caller: this runs on migrationLoop's own goroutine, which
// still has other shards (and Config polling, on a different goroutine
// entirely) to get to.
func (s *GroupServer) pullFrom(donors []*GroupServer, shard, configNum int) (PullReply, bool) {
	args := &PullArgs{Shard: shard, ConfigNum: configNum}
	for _, donor := range donors {
		var reply PullReply
		if err := donor.Pull(args, &reply); err != nil {
			continue
		}
		if reply.Err == GroupOK {
			return reply, true
		}
	}
	return PullReply{}, false
}

// gcLoop is migrationLoop's mirror image on the giving-away side: once per
// tick, a leader checks for any shard still in leaving, asks the new owner
// (via HasShard) whether it's actually ready yet, and Proposes a GC entry
// once it is. Fire-and-forget for the identical reason migrationLoop's
// Migrate Propose is: GC is idempotent (see applyLoop), so a lost proposal
// just gets retried next tick.
//
// This is deliberately conservative, not maximally prompt: if the recipient
// named in leaving[shard] never confirms (its own migrationLoop is stuck, it
// crashed for good, or — a real, accepted gap — the shard was reassigned
// AGAIN before it ever got the chance), this group just keeps the shard's
// data forever rather than risk deleting the only other copy while it might
// not actually be usable yet. leaving[shard] itself is only ever set by
// THIS group's own Config transitions (applyLoop), so once this group gives
// a shard away, it stops tracking where the shard goes next — it keeps
// asking the ORIGINAL recipient, never learns of a later hand-off further
// down the chain, and never collects if that recipient also moves the shard
// on before confirming. Safe (never deletes early), not maximally live —
// TASKS.md's own Day 6 framing is "without it, every migration leaks the
// shard forever," and this closes the common case without claiming to
// solve arbitrary reassignment chains in one day.
func (s *GroupServer) gcLoop() {
	ticker := time.NewTicker(groupGCPollInterval)
	defer ticker.Stop()

	proposed := make(map[int]bool)
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			if s.rf.State() != raft.Leader {
				proposed = make(map[int]bool)
				continue
			}
			s.mu.Lock()
			pending := make(map[int]int, len(s.leaving))
			for shard, to := range s.leaving {
				pending[shard] = to
			}
			peers := s.peers
			s.mu.Unlock()

			for shard, toGID := range pending {
				if proposed[shard] {
					continue
				}
				if !s.recipientReady(peers[toGID], shard) {
					continue
				}
				if _, _, isLeader := s.rf.Propose(groupOp{Type: "GC", Shard: shard}); isLeader {
					proposed[shard] = true
				}
			}
		}
	}
}

// recipientReady asks each replica of the new owner in turn (round-robin-
// until-success, same shape as pullFrom) whether it's fully ready to serve
// shard — true only once some replica actually confirms it.
func (s *GroupServer) recipientReady(recipients []*GroupServer, shard int) bool {
	args := &HasShardArgs{Shard: shard}
	for _, r := range recipients {
		var reply HasShardReply
		if err := r.HasShard(args, &reply); err != nil {
			continue
		}
		if reply.Err == GroupOK && reply.Ready {
			return true
		}
	}
	return false
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

// Pull answers a NEW owner's request for one shard's frozen data — called
// on the group that used to (or may still) own it, not gated by Get/
// PutAppend's own leader-and-noop rule, because ANY replica that has
// applied through args.ConfigNum has an identical, stable answer: once this
// replica's own cfg reaches that version, its own apply-time ownership
// check (applyLoop's Put/Append case) permanently refuses that shard from
// then on, so nothing can mutate it further — the data is frozen, whether
// this replica happens to be leader right now or not.
//
// Two ways to say "not yet": s.cfg.Num < args.ConfigNum means this replica
// hasn't itself caught up to the transition that would freeze the shard —
// answering anyway could hand back a snapshot missing a write still about
// to land. Being asked for a shard this replica is ITSELF still migrating
// in (still in s.migrating) means its own copy isn't complete yet either,
// even though cfg may already have caught up — answering with what's there
// would silently hand off partial data. Both cases return GroupErrNotReady;
// the caller's migrationLoop just retries next tick, no different from any
// other polling loop in this stage.
func (s *GroupServer) Pull(args *PullArgs, reply *PullReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cfg.Num < args.ConfigNum {
		reply.Err = GroupErrNotReady
		return nil
	}
	if _, stillMigrating := s.migrating[args.Shard]; stillMigrating {
		reply.Err = GroupErrNotReady
		return nil
	}

	data := make(map[string]string)
	for k, v := range s.store {
		if Key2Shard(k) == args.Shard {
			data[k] = v
		}
	}
	dup := make(map[int64]int64, len(s.duplicateTable))
	for clientID, seq := range s.duplicateTable {
		dup[clientID] = seq
	}

	reply.Data = data
	reply.DupTable = dup
	reply.Err = GroupOK
	return nil
}

// HasShard answers the old owner's gcLoop: is THIS group actually ready to
// serve shard (owns it per cfg AND isn't still migrating it in)? Not gated
// by leader-and-noop the way Get is, for the same reason Pull isn't — the
// fact "this replica considers itself ready" is exactly ownsLocked's own
// check, and it's already only ever true once the identical guarantee
// Get's gate exists to provide already holds, via cfg/migrating themselves
// only ever advancing through the committed log.
func (s *GroupServer) HasShard(args *HasShardArgs, reply *HasShardReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, stillMigrating := s.migrating[args.Shard]
	reply.Ready = s.cfg.Shards[args.Shard] == s.gid && !stillMigrating
	reply.Err = GroupOK
	return nil
}
