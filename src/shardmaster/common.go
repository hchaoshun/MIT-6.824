package shardmaster

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

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
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

const (
	OK = "OK"
)

type Err string

type JoinArgs struct {
	clientId		int64
	requestSeq		int
	Servers 		map[int][]string // new GID -> servers mappings
}

func (arg *JoinArgs) copy() JoinArgs {
	newArgs := JoinArgs{Servers:make(map[int][]string)}
	for gid, servers := range arg.Servers {
		newArgs.Servers[gid] = servers
	}
	return newArgs
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	clientId		int64
	requestSeq		int
	GIDs 			[]int
}

func (arg *LeaveArgs) copy() LeaveArgs {
	newArgs := LeaveArgs{GIDs:arg.GIDs}
	return newArgs
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	clientId		int64
	requestSeq		int
	Shard 			int
	GID   			int
}

func (arg *MoveArgs) copy() MoveArgs {
	return MoveArgs{Shard:arg.Shard, GID:arg.GID}
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
}

func (arg *QueryArgs) copy() QueryArgs {
	return QueryArgs{Num:arg.Num}
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
