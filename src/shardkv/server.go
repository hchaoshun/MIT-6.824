package shardkv


// import "shardmaster"
import (
	"bytes"
	"labrpc"
	"log"
	"shardmaster"
	"time"
)
import "raft"
import "sync"
import "labgob"

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
	Term 		int
	Err         Err
	Value       string
}

type ShardKV struct {
	sync.Mutex
	me           	int
	rf           	*raft.Raft
	applyCh      	chan raft.ApplyMsg
	make_end     	func(string) *labrpc.ClientEnd
	gid          	int
	masters      	[]*labrpc.ClientEnd
	maxraftstate 	int // snapshot if log grows this big
	persister 		*raft.Persister

	mck 			*shardmaster.Clerk //shardmaster 的client端
	config			shardmaster.Config //存储最近的config

	shutdown 		chan struct{}
	data 			map[string]string
	cache			map[int64]int
	notifyChanMap	map[int]chan NotifyMsg
}

func (kv *ShardKV) snapshot(lastCommandIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.config)
	e.Encode(kv.data)
	e.Encode(kv.cache)
	snapshot := w.Bytes()
	//需要修改log和lastincludedindex，所以此函数在raft层实现
	kv.rf.PersistAndSaveSnapshot(lastCommandIndex, snapshot)
}

func (kv *ShardKV) readSnapshot() {
	snapshot := kv.persister.ReadSnapshot()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.config) != nil ||
		d.Decode(&kv.data) != nil ||
		d.Decode(&kv.cache) != nil {
		log.Fatal("error while unmarshal snapshot.")
	}

}

func (kv *ShardKV) notifyIfPresent(index int, reply NotifyMsg) {
	if ch, ok := kv.notifyChanMap[index]; ok {
		ch <- reply
		delete(kv.notifyChanMap, index)
	}
}

func(kv *ShardKV) snapshotIfNeed(lastCommandIndex int) {
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
		kv.snapshot(lastCommandIndex)
	}
}

func (kv *ShardKV) Start(configNum int, command interface{}) (Err, string) {
	kv.Lock()
	defer kv.Unlock()
	//client 的confignum于server的不一致
	if configNum != kv.config.Num {
		return ErrWrongGroup, ""
	}

	//立即返回
	index, term, ok := kv.rf.Start(command)
	if !ok {
		return ErrWrongLeader, ""
	}
	//TODO 缓冲足够?
	notifyCh := make(chan NotifyMsg)
	kv.notifyChanMap[index] = notifyCh
	kv.Unlock()
	select {
	case msg := <-notifyCh:
		kv.Lock()
		if msg.Term != term {
			return ErrWrongLeader, ""
		} else {
			return msg.Err, msg.Value
		}
	case <-time.After(StartTimeoutInterval):
		kv.Lock()
		delete(kv.notifyChanMap, index)
		return ErrTimeout, ""
	}
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	reply.Err, reply.Value = kv.Start(args.ConfigNum, args.copy())
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	reply.Err, _ = kv.Start(args.ConfigNum, args.copy())
}

func (kv *ShardKV) apply(msg raft.ApplyMsg) {
	result := NotifyMsg{Term:msg.CommandTerm, Err:"OK", Value:""}
	//todo
	if arg, ok := msg.Command.(GetArgs); ok {
		result.Value = kv.data[arg.Key]
	}
	kv.notifyIfPresent(msg.CommandIndex, result)
	kv.snapshotIfNeed(msg.CommandIndex)

}

func(kv *ShardKV) run() {
	//todo
	go kv.rf.Replay(1)
	for {
		select {
		case msg := <-kv.applyCh:
			kv.Lock()
			if msg.CommandValid {
				kv.apply(msg)
			} else if cmd, ok := msg.Command.(string); ok {
				if cmd == "InstallSnapshot" {
					kv.readSnapshot()
				}
				//todo newleader?
			}
			kv.Unlock()
		case <-kv.shutdown:
			return
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int,
	gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.persister = persister
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.config = shardmaster.Config{}
	kv.shutdown = make(chan struct{})
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = make(map[string]string)
	kv.cache = make(map[int64]int)
	kv.notifyChanMap = make(map[int]chan NotifyMsg)

	go kv.run()

	return kv
}
