package shardkv

// CtrlerErr is a shard-controller RPC's outcome code — the same small,
// closed vocabulary 02-kv-store's Err uses, for the same reason: the client
// (CtrlerClerk) only ever needs to know "succeeded," "try someone else," or
// "gave up waiting," never a free-form error string.
type CtrlerErr string

const (
	CtrlerOK             CtrlerErr = "OK"
	CtrlerErrWrongLeader CtrlerErr = "ErrWrongLeader"
	CtrlerErrTimeout     CtrlerErr = "ErrTimeout"

	// CtrlerErrInvalidArgs means the request was rejected before ever being
	// proposed — a shard number outside [0, NShards), or a Move naming a
	// group id not present in the CURRENT config. It's checked against a
	// snapshot of the config taken right before Propose, not re-checked
	// against whatever config the entry actually lands next to once
	// committed — a group could theoretically Leave in the gap between the
	// check and commit, letting a Move land on a now-dead group. That
	// narrow window is a known, accepted gap for this day's scope (see
	// README); closing it needs the check to happen at APPLY time, inside
	// applyLoop, which would need a way to report "committed but rejected"
	// back through the notify channel — more machinery than this day's
	// pure-config-history scope calls for.
	CtrlerErrInvalidArgs CtrlerErr = "ErrInvalidArgs"
)

type JoinArgs struct {
	Groups map[int][]string

	ClientID int64
	SeqNum   int64
}

type JoinReply struct {
	Err CtrlerErr
}

type LeaveArgs struct {
	GIDs []int

	ClientID int64
	SeqNum   int64
}

type LeaveReply struct {
	Err CtrlerErr
}

type MoveArgs struct {
	Shard int
	GID   int

	ClientID int64
	SeqNum   int64
}

type MoveReply struct {
	Err CtrlerErr
}

// QueryArgs.Num selects which config version to return; -1 (or any number
// >= the latest known version) means "the latest," matching MIT 6.5840's
// shardctrler convention.
type QueryArgs struct {
	Num int
}

type QueryReply struct {
	Err    CtrlerErr
	Config Config
}
