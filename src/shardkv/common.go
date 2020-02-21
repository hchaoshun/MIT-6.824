package shardkv

import "time"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup" // todo client和server如何处理此错误？
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout = "ErrTimeout"
)

const StartTimeoutInterval = time.Duration(3 * time.Second)


type Err string

// Put or Append
type PutAppendArgs struct {
	Key   			string
	Value 			string
	Op    			string // "Put" or "Append"
	ConfigNum		int
	ClientId		int64
	RequestSeq		int
}

type PutAppendReply struct {
	Err         Err
}

func (arg *PutAppendArgs) copy() PutAppendArgs {
	newArgs := PutAppendArgs{Key:arg.Key, Value:arg.Value, Op:arg.Op, ConfigNum:arg.ConfigNum,
		ClientId:arg.ClientId, RequestSeq:arg.RequestSeq}
	return newArgs
}

type GetArgs struct {
	Key 			string
	ConfigNum		int
}

type GetReply struct {
	Err         Err
	Value       string
}

func (arg *GetArgs) copy() GetArgs {
	newArgs := GetArgs{Key:arg.Key, ConfigNum:arg.ConfigNum}
	return newArgs
}
