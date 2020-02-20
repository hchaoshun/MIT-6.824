package shardmaster

import "log"

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


// The number of shards.
//shard数量固定，类似于一致性hash算法
const NShards = 10

//不同shard值gid可能相同，让gid均匀分布在shards数组中，gid的数量差 < 2
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (conf *Config) copy() Config {
	newConf := Config{Num:conf.Num, Shards:conf.Shards, Groups:make(map[int][]string)}
	for gid, servers := range conf.Groups {
		//todo 不用append是否正确
		newConf.Groups[gid] = servers
	}
	return newConf
}

//todo 和kvraft合并
type Err string
const (
	OK = "OK"
	WrongLeader = "WrongLeader"
	Timeout = "ErrTimeout"
)

type JoinArgs struct {
	ClientId		int64
	RequestSeq		int
	Servers 		map[int][]string // new GID -> servers mappings
}

func (arg *JoinArgs) copy() JoinArgs {
	newArgs := JoinArgs{ClientId:arg.ClientId, RequestSeq:arg.RequestSeq, Servers:make(map[int][]string)}
	for gid, servers := range arg.Servers {
		newArgs.Servers[gid] = servers
	}
	return newArgs
}

type JoinReply struct {
	Err         Err
}

type LeaveArgs struct {
	ClientId		int64
	RequestSeq		int
	GIDs 			[]int
}

func (arg *LeaveArgs) copy() LeaveArgs {
	newArgs := LeaveArgs{ClientId:arg.ClientId, RequestSeq:arg.RequestSeq, GIDs:arg.GIDs}
	return newArgs
}

type LeaveReply struct {
	Err         Err
}

type MoveArgs struct {
	ClientId		int64
	RequestSeq		int
	Shard 			int
	GID   			int
}

func (arg *MoveArgs) copy() MoveArgs {
	return MoveArgs{ClientId:arg.ClientId, RequestSeq:arg.RequestSeq, Shard:arg.Shard, GID:arg.GID}
}

type MoveReply struct {
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
}

func (arg *QueryArgs) copy() QueryArgs {
	return QueryArgs{Num:arg.Num}
}

type QueryReply struct {
	Err         Err
	Config      Config
}
