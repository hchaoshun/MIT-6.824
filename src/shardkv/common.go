package shardkv

import "time"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each Shard.
// Shardmaster may change Shard assignment from time to time.
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

type IntSet map[int]struct{}
type Err string
type MigrationData struct {
	Data	map[string]string
	Cache 	map[int64]string
}

// Put or Append
type PutAppendArgs struct {
	Key   			string
	Value 			string
	Op    			string // "Put" or "Append"
	ConfigNum		int
	RequestId		int64
	ExpireRequestId	int64
}

type PutAppendReply struct {
	Err         Err
}

func (arg *PutAppendArgs) Copy() PutAppendArgs {
	newArgs := PutAppendArgs{Key:arg.Key, Value:arg.Value, Op:arg.Op, ConfigNum:arg.ConfigNum,
		RequestId:arg.RequestId, ExpireRequestId:arg.ExpireRequestId}
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

func (arg *GetArgs) Copy() GetArgs {
	newArgs := GetArgs{Key:arg.Key, ConfigNum:arg.ConfigNum}
	return newArgs
}

type ShardMigrationArgs struct {
	Shard     int
	ConfigNum int
}

type ShardMigrationReply struct {
	Err           Err
	Shard         int //返回的shard用于加入client的ownShards
	ConfigNum     int //返回的configNum用于和client判断是否正确
	MigrationData MigrationData
}

type ShardCleanArgs struct {
	Shard 		int
	ConfigNum 	int
}

type ShardCleanReply struct {
	Shard 		int
	ConfigNum	int
	Err			Err
}

func (args *ShardCleanReply) Copy() ShardCleanReply {
	newArgs := ShardCleanReply{Shard:args.Shard, ConfigNum:args.ConfigNum}
	return newArgs
}


