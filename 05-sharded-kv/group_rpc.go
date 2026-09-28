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
