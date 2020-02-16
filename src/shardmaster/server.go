package shardmaster


import (
	"log"
	"raft"
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

type Op struct {
	// Your data here.
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
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
