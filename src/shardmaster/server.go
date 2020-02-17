package shardmaster

import (
	"log"
	"raft"
	"time"
)
import "labrpc"
import "sync"
import "labgob"

//必须注册，否则报空指针异常
func init() {
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}

type Op struct {
	// Your data here.
}

type NotifyArg struct {
	Term		int
	Value		interface{} //Query将config传给value
	Err 		Err
}

type ShardMaster struct {
	sync.Mutex
	me      		int
	rf      		*raft.Raft
	applyCh 		chan raft.ApplyMsg
	shutdown		chan struct{}
	cache			map[int64]int
	notifyChanMap	map[int]chan NotifyArg
	configs 		[]Config // indexed by config num
}

func (sm *ShardMaster) getConfig(num int) Config {
	var srcConfig Config
	if num < 0 || num >= len(sm.configs) - 1 {
		srcConfig = sm.configs[len(sm.configs) - 1]
	}  else {
		srcConfig = sm.configs[num]
	}
	dstConfig := Config{Num:srcConfig.Num, Shards:srcConfig.Shards, Groups:make(map[int][]string)}
	for gid, servers := range srcConfig.Groups {
		dstConfig.Groups[gid] = servers
	}
	return dstConfig
}

func (sm *ShardMaster) notifyIfPresent(index int, reply NotifyArg) {
	if ch, ok := sm.notifyChanMap[index]; ok {
		ch <- reply
	}
	delete(sm.notifyChanMap, index)
}

//返回的第二个参数是config
func (sm *ShardMaster) start(command interface{}) (Err, interface{}) {
	index, term, ok := sm.rf.Start(command)
	if !ok {
		return WrongLeader, struct {}{}
	}
	sm.Lock()
	notifyCh := make(chan NotifyArg)
	sm.notifyChanMap[index] = notifyCh
	sm.Unlock()

	select {
	case msg := <- notifyCh:
		if msg.Term != term {
			return WrongLeader, struct {}{}
		} else {
			return OK, msg.Value
		}
	//todo 时间合并
	case <-time.After(time.Duration(3 * time.Second)):
		sm.Lock()
		delete(sm.notifyChanMap, index)
		sm.Unlock()
		return Timeout, struct {}{}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	reply.Err, _ = sm.start(args.copy())
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	reply.Err, _ = sm.start(args.copy())
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	reply.Err, _ = sm.start(args.copy())
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	sm.Lock()
	if args.Num > 0 && args.Num < len(sm.configs) {
		reply.Err, reply.Config = OK, sm.getConfig(args.Num)
		sm.Unlock()
		return
	} else {
		sm.Unlock()
		err, config := sm.start(args.copy())
		reply.Err = err
		if err == OK {
			reply.Config = config.(Config)
		}
	}
}

func (sm *ShardMaster) appendNewConfig(config Config) {
	config.Num = len(sm.configs)
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) apply(msg raft.ApplyMsg) {
	result := NotifyArg{Term:msg.CommandTerm, Err:OK, Value:""}
	if arg, ok := msg.Command.(QueryArgs); ok {
		result.Value = sm.getConfig(arg.Num)
	} else if arg, ok := msg.Command.(MoveArgs); ok {
		if sm.cache[arg.ClientId] < arg.RequestSeq {
			newConfig := sm.getConfig(-1)
			newConfig.Shards[arg.Shard] = arg.GID
			sm.cache[arg.ClientId] = arg.RequestSeq
			sm.appendNewConfig(newConfig)
		}
	}
	sm.notifyIfPresent(msg.CommandIndex, result)
}

func (sm *ShardMaster) run() {
	for {
		select {
		case msg := <-sm.applyCh:
			sm.Lock()
			if msg.CommandValid {
				sm.apply(msg)
			}
			//todo else ?
			sm.Unlock()
		case <-sm.shutdown:
			return
		}
	}
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	//todo 缓冲是否足够?
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.cache = make(map[int64]int)
	sm.shutdown = make(chan struct{})
	sm.notifyChanMap = make(map[int]chan NotifyArg)

	go sm.run()
	return sm
}
