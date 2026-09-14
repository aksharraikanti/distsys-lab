package kvstore

// Err is a KV RPC's outcome code.
type Err string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrTimeout     Err = "ErrTimeout"
)

// GetArgs/GetReply and PutAppendArgs/PutAppendReply are this stage's
// client-facing RPCs — deliberately shaped like Stage 1's RequestVote/
// AppendEntries pairs (an Args struct in, a Reply struct out, an Err
// field telling the client what happened) for the same reason: one
// uniform RPC shape across the whole codebase, not a new convention
// invented per package.
type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
}

type PutAppendReply struct {
	Err Err
}
