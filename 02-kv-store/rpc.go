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

	// ClientID/SeqNum identify this request for duplicate detection (Day
	// 3) — see Op's doc comment for the exact contract. The caller (a
	// real client library, or a test standing in for one) owns
	// generating a stable ClientID and a strictly increasing SeqNum per
	// new logical request; PutAppend does not invent these itself.
	ClientID int64
	SeqNum   int64
}

type PutAppendReply struct {
	Err Err
}
