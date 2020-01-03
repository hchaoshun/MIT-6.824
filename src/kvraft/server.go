package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

//必须注册，否则报空指针异常
func init() {
	labgob.Register(GetArgs{})
	labgob.Register(PutAppendArgs{})
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type NotifyMsg struct {
	Err         Err
	Value       string
}

type KVServer struct {
	sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big
	shutdown 		chan struct{}
	data			map[string]string
	cache			map[int64]int
	notifyChanMap 	map[int]chan NotifyMsg
}

func (kv *KVServer) notifyIfPresent(index int, reply NotifyMsg) {
	if ch, ok := kv.notifyChanMap[index]; ok {
		ch <- reply
		delete(kv.notifyChanMap, index)
	}
}

func (kv *KVServer) Start(command interface{}) (Err, string) {
	DPrintf("call KVServer start")
	//todo
	index, _, ok := kv.rf.Start(command)
	if !ok {
		DPrintf("is not leader, so start return")
		return ErrWrongLeader, ""
	}
	kv.Lock()
	notifyCh := make(chan NotifyMsg)
	kv.notifyChanMap[index] = notifyCh
	kv.Unlock()
	DPrintf("wait notifyCh read or timeout")
	select {
	case msg := <-notifyCh:
		DPrintf("%v notifyCh received msg.", kv.me)
		return msg.Err, msg.Value
	//必须设置超时，否则会永久阻塞
	case <-time.After(StartTimeoutInterval):
		kv.Lock()
		delete(kv.notifyChanMap, index)
		kv.Unlock()
		DPrintf("%v notifyCh received msg timeout.", kv.me)
		return ErrTimeout, ""
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("rpc call Get, args: %v", *args)
	reply.Err, reply.Value = kv.Start(args.copy())
	DPrintf("rpc call Get, args: %v return", *args)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("rpc call PutAppend, args: %v", *args)
	reply.Err, _ = kv.Start(args.copy())
	DPrintf("rpc call PutAppend, args: %v return", *args)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func(kv *KVServer) apply(msg raft.ApplyMsg) {
	result := NotifyMsg{Err:"OK", Value:""}
	if arg, ok := msg.Command.(GetArgs); ok {
		//读操作没必要缓存和检查是否是上次retry
		result.Value = kv.data[arg.Key]
	} else if arg, ok := msg.Command.(PutAppendArgs); ok {
		if kv.cache[arg.ClientId] < arg.RequestSeq {
			if arg.Op == "Put" {
				kv.data[arg.Key] = arg.Value
			} else if arg.Op == "Append" {
				kv.data[arg.Key] += arg.Value
			}
			kv.cache[arg.ClientId] = arg.RequestSeq
		}
	} else {
		result.Err = ErrWrongLeader
	}
	DPrintf("%v send result: to notifyCh", kv.me)
	kv.notifyIfPresent(msg.CommandIndex, result)
}

func(kv *KVServer) run() {
	//DPrintf("run server %v", kv.me)
	for {
		//DPrintf("%v applyCh wait receive...", kv.me)
		select {
		case msg := <-kv.applyCh:
			//接收到此消息一定是leader
			DPrintf("%v applyCh received msg.", kv.me)
			if msg.CommandValid {
				kv.apply(msg)
			} //todo else ?
		case <-kv.shutdown:
			return
		}
	}
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.shutdown = make(chan struct{})
	kv.data = make(map[string]string)
	kv.cache = make(map[int64]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.notifyChanMap = make(map[int]chan NotifyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.run()
	return kv
}
