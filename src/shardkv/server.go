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
	me           		int
	rf           		*raft.Raft
	applyCh      		chan raft.ApplyMsg
	make_end     		func(string) *labrpc.ClientEnd
	gid          		int
	masters      		[]*labrpc.ClientEnd
	maxraftstate 		int // snapshot if log grows this big
	persister 			*raft.Persister
	mck 				*shardmaster.Clerk //shardmaster 的client端,只有leader才能和它通信
	config				shardmaster.Config //存储当前的config

	ownShards			IntSet //此group在config.Shards中的分布情况，范围是0~NShards
	//map[int]MigrationData中，key是此gid中要迁移走的shard，value是此gid中要迁移走的shard对应的数据
	migratingShards		map[int]map[int]MigrationData
	//key是从其他gid迁移到此gid的shard
	waitingShards		map[int]int
	historyConfigs		[]shardmaster.Config


	shutdown 			chan struct{}
	data 				map[string]string
	cache				map[int64]string
	notifyChanMap		map[int]chan NotifyMsg
}

func (kv *ShardKV) snapshot(lastCommandIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.config)
	e.Encode(kv.data)
	e.Encode(kv.cache)
	e.Encode(kv.ownShards)
	e.Encode(kv.migratingShards)
	e.Encode(kv.waitingShards)
	e.Encode(kv.historyConfigs)
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
		d.Decode(&kv.cache) != nil ||
		d.Decode(&kv.ownShards) != nil ||
		d.Decode(&kv.migratingShards) != nil ||
		d.Decode(&kv.waitingShards) != nil ||
		d.Decode(&kv.historyConfigs) != nil {
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

//定时向shardmaster获取最新config
func (kv *ShardKV) poll() {
	kv.Lock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		kv.Unlock()
		return
	}
	nextConfigNum := kv.config.Num + 1
	kv.Unlock()
	newConfig := kv.mck.Query(nextConfigNum)
	//条件成立说明shardmaster已经更新了config，此时kv也要更新config
	//条件不成立说明之前的config是最新的，此时什么也不做
	if newConfig.Num == nextConfigNum {
		kv.rf.Start(newConfig)
	}
}

//可能会将其他gid的shard迁移进来，或/和将shard迁移到其他gid
//考虑old:[1:6, 2:6, 3:6, 4:6, 5:8, 6:8]
//   new:[1:6, 2:6, 3:7, 4:7, 5:6, 6:6]
//其中[3,4]是将要迁移走的，存放在migratingShards
//[5,6]是将要迁移进来的，存放在waitingShards
func (kv *ShardKV) appendNewConfig(newConfig shardmaster.Config) {
	//新的config 的num必须大于原来的
	if newConfig.Num <= kv.config.Num {
		return
	}

	oldConfig, oldOwnShards := kv.config, kv.ownShards
	kv.historyConfigs = append(kv.historyConfigs, oldConfig)
	oldNum := oldConfig.Num
	kv.config, kv.ownShards = newConfig, make(IntSet)

	for shard, gid := range newConfig.Shards {
		if gid == kv.gid {
			if _, ok := oldOwnShards[shard]; ok {
				kv.ownShards[shard] = struct{}{}
				//删除完最后剩下的是已经被其他gid替换的，此时需要迁移走
				delete(oldOwnShards, shard)
			} else {
				//满足gid == kv.gid且不在oldOwnShards里的就是新加的且需要迁移进来的shard
				kv.waitingShards[shard] = oldNum
			}
		}
	}

	if len(oldOwnShards) != 0 {
		migrateData := make(map[int]MigrationData)
		for shard := range oldOwnShards {
			data := MigrationData{Data:make(map[string]string), Cache:make(map[int64]int)}
			for k, v := range kv.data {
				if key2shard(k) == shard {
					data.Data[k] = v
					delete(kv.data, k)
				}

				//todo how to handle cache
			}
			migrateData[shard] = data
		}
		kv.migratingShards[oldNum] = migrateData
	}

}



func (kv *ShardKV) apply(msg raft.ApplyMsg) {
	result := NotifyMsg{Term:msg.CommandTerm, Err:"OK", Value:""}
	if arg, ok := msg.Command.(GetArgs); ok {
		shard := key2shard(arg.Key)
		if _, ok := kv.ownShards[shard]; !ok {
			result.Err = ErrWrongGroup
		} else if arg.ConfigNum != kv.config.Num {
			result.Err = ErrWrongGroup
		} else {
			result.Value = kv.data[arg.Key]
		}
	} else if arg, ok := msg.Command.(PutAppendArgs); ok {
		shard := key2shard(arg.Key)
		if _, ok := kv.ownShards[shard]; !ok {
			result.Err = ErrWrongGroup
		} else if arg.ConfigNum != kv.config.Num {
			result.Err = ErrWrongGroup
		//TODO cache缓存是否正确?
		} else if _, ok := kv.cache[arg.RequestId]; !ok {
			if arg.Op == "Put" {
				kv.data[arg.Key] = arg.Value
			} else {
				kv.data[arg.Key] += arg.Value
			}
			delete(kv.cache, arg.ExpireRequestId)
			kv.cache[arg.RequestId] = arg.Key
		}
	} else if arg, ok := msg.Command.(shardmaster.Config); ok {
		kv.appendNewConfig(arg)
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

	kv.ownShards = make(IntSet)
	kv.migratingShards = make(map[int]map[int]MigrationData)
	kv.waitingShards = make(map[int]int)
	kv.historyConfigs = make([]shardmaster.Config, 0)

	kv.data = make(map[string]string)
	kv.cache = make(map[int64]string)
	kv.notifyChanMap = make(map[int]chan NotifyMsg)

	go kv.run()

	return kv
}
