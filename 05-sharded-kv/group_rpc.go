package shardkv

// GroupErr is a shard-group RPC's outcome code — 02-kv-store's Err, plus
// GroupErrWrongGroup: this server is Raft leader and otherwise ready to
// answer, but the key's shard isn't assigned to this group under the
// config it has actually applied. A client sees this exactly where it
// would see ErrWrongLeader from a plain KVServer, and reacts the same way
// in spirit — try again elsewhere — except "elsewhere" means refetching the
// current config from the controller, not just trying the next replica of
// the same group.
type GroupErr string

const (
	GroupOK             GroupErr = "OK"
	GroupErrNoKey       GroupErr = "ErrNoKey"
	GroupErrWrongLeader GroupErr = "ErrWrongLeader"
	GroupErrWrongGroup  GroupErr = "ErrWrongGroup"
	GroupErrTimeout     GroupErr = "ErrTimeout"

	// GroupErrNotReady is Pull's own answer: this replica hasn't yet
	// applied the Config entry that revoked its ownership of the requested
	// shard, so its data for that shard isn't frozen yet — some write still
	// in flight could land after this snapshot was taken. See Pull's doc
	// comment for why waiting for that, rather than serving early, is what
	// makes a migrated shard's data complete.
	GroupErrNotReady GroupErr = "ErrNotReady"
)

type GroupGetArgs struct {
	Key string
}

type GroupGetReply struct {
	Err   GroupErr
	Value string
}

type GroupPutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"

	ClientID int64
	SeqNum   int64
}

type GroupPutAppendReply struct {
	Err GroupErr
}

// PullArgs asks a (possibly former) owner for one shard's frozen data.
// ConfigNum is the config version AT WHICH the caller gained the shard —
// the donor only answers once its OWN applied config has reached at least
// that version (GroupErrNotReady otherwise), which is what guarantees the
// data handed back is complete. See Pull's doc comment.
type PullArgs struct {
	Shard     int
	ConfigNum int
}

type PullReply struct {
	Err      GroupErr
	Data     map[string]string
	DupTable map[int64]int64
}
