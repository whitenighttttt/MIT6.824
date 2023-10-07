package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"

	ErrKilled 		= "ErrKilled"
	ErrDuplicate	= "ErrDuplicate"
	ErrTimeout		= "ErrTimeout"
)

const(
	PutOp 			= "PutOp"
	AppendOp		= "AppendOp"
	GetOp			= "GetOp"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId	int64
	SeqId		int	// 当前序列号，放出的第几个请求
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId	int64
	SeqId		int
}

type GetReply struct {
	Err   Err
	Value string
}
