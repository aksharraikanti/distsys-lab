package raft

// RPC argument/reply types for the two Raft RPCs. These match the shapes
// defined in the Raft paper (Ongaro & Ousterhout, Figure 2) — election
// (RequestVote) and replication/heartbeats (AppendEntries).

// RequestVoteArgs is sent by a candidate to solicit a vote.
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateID  int // candidate requesting the vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// RequestVoteReply is a voter's response to a RequestVote RPC.
type RequestVoteReply struct {
	Term        int  // currentTerm, for the candidate to update itself
	VoteGranted bool // true means the candidate received the vote
}

// LogEntry is one entry in a Raft node's replicated log.
type LogEntry struct {
	Term    int         // term when the entry was received by the leader
	Command interface{} // the state-machine command
}

// AppendEntriesArgs is sent by the leader both to replicate log entries and,
// with an empty Entries slice, as a heartbeat.
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderID     int        // so followers can redirect clients
	PrevLogIndex int        // index of the log entry immediately preceding new ones
	PrevLogTerm  int        // term of PrevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commitIndex
}

// AppendEntriesReply is a follower's response to an AppendEntries RPC.
type AppendEntriesReply struct {
	Term    int  // currentTerm, for the leader to update itself
	Success bool // true if the follower contained an entry matching PrevLogIndex/PrevLogTerm
}

// InstallSnapshotArgs is sent by the leader to catch up a follower whose
// nextIndex has fallen at or below the leader's own lastIncludedIndex
// (Day 6) — the entries that follower needs no longer exist anywhere in
// the leader's log, so AppendEntries can never succeed for it; the
// leader sends its entire compacted state in one shot instead.
//
// Simplified from the Raft paper's version (§7): real Raft supports
// chunked transfer (Offset/Done fields) for snapshots too large for one
// RPC. At this project's in-memory, test-cluster scale a snapshot is
// always small enough to send whole — the same "simplest correct thing,
// not the fastest" call this project already made for persistence (see
// persist.go).
type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderID          int    // so followers can redirect clients
	LastIncludedIndex int    // the snapshot replaces the log through this index
	LastIncludedTerm  int    // term of LastIncludedIndex
	Data              []byte // the state machine's serialized snapshot, opaque to Raft
}

// InstallSnapshotReply is a follower's response to an InstallSnapshot RPC.
type InstallSnapshotReply struct {
	Term int // currentTerm, for the leader to update itself
}
