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
