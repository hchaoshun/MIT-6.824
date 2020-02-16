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
	Value		string
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

func (sm *ShardMaster) notifyIfPresent(index int, reply NotifyArg) {
	if ch, ok := sm.notifyChanMap[index]; ok {
		ch <- reply
	}
	delete(sm.notifyChanMap, index)
}

func (sm *ShardMaster) start(command interface{}) (Err, string) {
	index, term, ok := sm.rf.Start(command)
	if !ok {
		return WrongLeader, ""
	}
	sm.Lock()
	notifyCh := make(chan NotifyArg)
	sm.notifyChanMap[index] = notifyCh
	sm.Unlock()

	select {
	case msg := <- notifyCh:
		if msg.Term != term {
			return WrongLeader, ""
		} else {
			//todo 返回空?
			return OK, ""
		}
	//todo 时间合并
	case <-time.After(time.Duration(3 * time.Second)):
		sm.Lock()
		delete(sm.notifyChanMap, index)
		sm.Unlock()
		return Timeout, ""
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
	// Your code here.
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
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
	//todo handle msg.Command
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
