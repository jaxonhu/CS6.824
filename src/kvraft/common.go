package raftkv

import "time"

const (
	OK       			= "OK"
	ErrNoKey 			= "ErrNoKey"
	ErrWrongLeader 		= "ErrWrongLeader"
	ServerTimeOut 		= "ServerTimeOut"
	RaftTermError		= "RaftTermError"
	DuplicatedRequest  	= "DuplicatedRequest"
)

type Err string

const ClientTimeOut = time.Duration(125 * time.Millisecond)
const RequestTimeOut = time.Duration(2 * time.Second)

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId 	int64
	RequestSeq  int
}

func (arg *PutAppendArgs) copy() PutAppendArgs {
	return PutAppendArgs{arg.Key, arg.Value, arg.Op, arg.ClientId, arg.RequestSeq}
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}



type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

func (arg *GetArgs) copy() GetArgs {
	return GetArgs{arg.Key}
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
