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
	labgob.Register(GetReply{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(ShardMigrationArgs{})
	labgob.Register(ShardMigrationReply{})
	labgob.Register(ShardCleanArgs{})
	labgob.Register(ShardCleanReply{})
	labgob.Register(MigrationData{})
	labgob.Register(shardmaster.Config{})

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
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
	//已经migrate的shard
	cleaningShards		map[int]IntSet
	historyConfigs		[]shardmaster.Config


	shutdown 			chan struct{}
	data 				map[string]string
	cache				map[int64]string
	notifyChanMap		map[int]chan NotifyMsg

	pollTimer			*time.Timer
	pullTimer			*time.Timer
	cleanTimer			*time.Timer
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

func (kv *ShardKV) getConfigNum() int {
	kv.Lock()
	defer kv.Unlock()
	return kv.config.Num
}

func (kv *ShardKV) Start(configNum int, command interface{}) (Err, string) {
	kv.Lock()
	defer kv.Unlock()
	//client 的confignum于server的不一致
	if configNum != kv.config.Num {
		DPrintf("server wrong group.command: %v client: %v, server: %v",command, configNum, kv.config.Num)
		return ErrWrongGroup, ""
	}

	//节点一致性操作
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
		DPrintf("server %v wait notifyCh.", msg)
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
	reply.Err, reply.Value = kv.Start(args.ConfigNum, args.Copy())
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("test put key: %v, ownshard: %v confignum: %v", args.Key, kv.ownShards, kv.config.Num)
	reply.Err, _ = kv.Start(args.ConfigNum, args.Copy())
}

//注意：server端无法判断迁移是否成功，所以此rpc不用同步到各节点
//client端迁移成功后会发出clean操作通知server删除已迁移的shard
//类似于tcp三次握手
//此调用是一个普通的rpc调用，不需要raft同步各节点
func (kv *ShardKV) ShardMigration(args *ShardMigrationArgs, reply *ShardMigrationReply) {
	kv.Lock()
	defer kv.Unlock()
	configNum := args.ConfigNum
	//不需要此server是leader，任何一个节点均可
	if configNum >= kv.config.Num {
		reply.Err = ErrWrongGroup
		return
	}
	reply.Err, reply.Shard, reply.ConfigNum, reply.MigrationData =
		OK, args.Shard, args.ConfigNum, MigrationData{Data: make(map[string]string), Cache:make(map[int64]string)}

	if v, ok := kv.migratingShards[configNum]; ok {
		if migrationData, ok := v[args.Shard]; ok {
			for k, v := range migrationData.Data {
				reply.MigrationData.Data[k] = v
			}
			for k, v := range migrationData.Cache {
				reply.MigrationData.Cache[k] = v
			}
		}
	}

}

//注意server的rpc调用函数并不会改变协议状态
func (kv *ShardKV) ShardClean(args *ShardCleanArgs, reply *ShardCleanReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongGroup
		return
	}
	reply.Shard, reply.ConfigNum = args.Shard, args.ConfigNum
	kv.Lock()
	defer kv.Unlock()
	if v, ok := kv.migratingShards[args.ConfigNum]; ok {
		if _, ok := v[args.Shard]; ok {
			kv.Unlock()
			//通知各个节点，删除migratingShards的数据
			err, _ := kv.Start(args.ConfigNum, reply.Copy())
			reply.Err = err
			kv.Lock()
		}
	}
}

//Leader定时向shardmaster获取最新config
func (kv *ShardKV) poll() {
	//DPrintf("start poll job.")
	kv.Lock()
	defer kv.pollTimer.Reset(PollInterval)
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
	//DPrintf("poll job finish all.")
}

//Leader作为client向server发出请求拉server数据
func (kv *ShardKV) pull() {
	//DPrintf("start pull job.")
	kv.Lock()
	defer kv.pullTimer.Reset(PullInterval)
	if _, isLeader := kv.rf.GetState(); !isLeader || len(kv.waitingShards) == 0 {
		kv.Unlock()
		return
	}

	ch, count := make(chan struct{}), 0
	for shard, configNum := range kv.waitingShards {
		go func(shard int, config shardmaster.Config) {
			kv.doPull(shard, config)
			ch <- struct{}{}
		}(shard, kv.historyConfigs[configNum].Copy())
		count++
	}
	kv.Unlock()

	for count > 0 {
		<-ch
		count--
	}
	//DPrintf("pull job finish")
}

func (kv *ShardKV) doPull(shard int, oldConfig shardmaster.Config) {
	gid := oldConfig.Shards[shard]
	if servers, ok := oldConfig.Groups[gid]; ok {
		args := ShardMigrationArgs{Shard: shard, ConfigNum:oldConfig.Num}
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply ShardMigrationReply
			ok := srv.Call("ShardKV.ShardMigration", &args, &reply)
			//只要有一个server成功执行就返回，不必关注server是否是leader
			if ok && reply.Err == OK {
				//拉回来的数据要同步到follower
				//注意：start参数confignum必须与server的confignum保持一致
				kv.Start(kv.getConfigNum(), reply)
				return
			}
		}
	}

}

//此操作发生在client已经把数据拉回成功之后，client向server发出clean rpc请求，通知server删除迁移后的shard
func (kv *ShardKV) clean() {
	//DPrintf("start clean job.")
	kv.Lock()
	defer kv.cleanTimer.Reset(cleanInterval)
	if _, isLeader := kv.rf.GetState(); !isLeader || len(kv.cleaningShards) == 0 {
		kv.Unlock()
		return
	}

	ch, count := make(chan struct{}), 0
	for configNum, shards := range kv.cleaningShards {
		oldConfig := kv.historyConfigs[configNum]
		for shard := range shards {
			go func(shard int, config shardmaster.Config) {
				kv.doClean(shard, config)
				ch <- struct{}{}
			}(shard, oldConfig)
			count++
		}
	}
	kv.Unlock()
	for count > 0 {
		<-ch
		count--
	}
	//DPrintf("clean job finish.")
}

func (kv *ShardKV) doClean(shard int, oldConfig shardmaster.Config) {
	gid := oldConfig.Shards[shard]
	if servers, ok := oldConfig.Groups[gid]; ok {
		args := ShardCleanArgs{Shard:shard, ConfigNum:oldConfig.Num}
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply ShardCleanReply
			ok := srv.Call("ShardKV.ShardClean", &args, &reply)
			//只有srv是leader才会成功
			if ok && reply.Err == OK {
				kv.Lock()
				delete(kv.cleaningShards[oldConfig.Num], shard)
				if len(kv.cleaningShards[oldConfig.Num]) == 0 {
					delete(kv.cleaningShards, oldConfig.Num)
				}
				kv.Unlock()
				return
			}
		}
	}
}

//shardmaster已经更新config
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
	kv.historyConfigs = append(kv.historyConfigs, oldConfig.Copy())
	oldNum := oldConfig.Num
	kv.config, kv.ownShards = newConfig.Copy(), make(IntSet)

	for shard, gid := range newConfig.Shards {
		if gid == kv.gid {
			//注意：初始化时kv.ownShards什么也没有，此时需要将新join的gid全部加入
			if _, ok := oldOwnShards[shard]; ok || oldConfig.Num == 0{
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
			data := MigrationData{Data:make(map[string]string), Cache:make(map[int64]string)}
			for k, v := range kv.data {
				if key2shard(k) == shard {
					data.Data[k] = v
					delete(kv.data, k)
				}

			}
			for k, v := range kv.cache {
				if key2shard(v) == shard {
					data.Cache[k] = v
					delete(kv.cache, k)
				}
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
			DPrintf("server kv.ownShards %v not shard %v", kv.ownShards, shard)
			result.Err = ErrWrongGroup
		} else if arg.ConfigNum != kv.config.Num {
			DPrintf("server arg.ConfigNum %v != kv.config.Num %v", arg.ConfigNum, kv.config.Num)
			result.Err = ErrWrongGroup
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
	} else if arg, ok := msg.Command.(ShardMigrationReply); ok {
		//用于迁移的configNum应该刚好是新的configNum的前一个
		//将waitingShards里的shard删除，然后加入ownShards和cleaningShards
		if arg.ConfigNum == kv.config.Num - 1 {
			delete(kv.waitingShards, arg.Shard)
			//todo 是否一定不在ownShards里？
			kv.ownShards[arg.Shard] = struct{}{}
			//执行到此步说明其他gid的shard已经成功迁移到此gid，此时需要通知server将迁移后的shard清理掉
			if _, ok := kv.cleaningShards[arg.ConfigNum]; !ok {
				kv.cleaningShards[arg.ConfigNum] = make(IntSet)
			}
			kv.cleaningShards[arg.ConfigNum][arg.Shard] = struct{}{}
			for k, v := range arg.MigrationData.Data {
				kv.data[k] = v
			}
			for k, v := range arg.MigrationData.Cache {
				kv.cache[k] = v
			}
		}
	} else if arg, ok := msg.Command.(ShardCleanReply); ok {
		if v, ok := kv.migratingShards[arg.ConfigNum]; ok {
			delete(v, arg.Shard)
			if len(v) == 0 {
				delete(kv.migratingShards, arg.ConfigNum)
			}
		}
	}
	kv.notifyIfPresent(msg.CommandIndex, result)
	kv.snapshotIfNeed(msg.CommandIndex)

}

//todo 在poll/pull/clean过程中均会占用lock导致不能正常提供服务，有没有办法解决?
func (kv *ShardKV) tick() {
	for {
		select {
		case <-kv.pollTimer.C:
			kv.poll()
		case <-kv.pullTimer.C:
			kv.pull()
		case <-kv.cleanTimer.C:
			kv.clean()
		case <-kv.shutdown:
			return
		}
	}
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
				//todo 放在这儿启动是否合适?
				} else if cmd == "ReplayDone" {
					go kv.tick()
				}
				//todo newleader?
			}
			kv.Unlock()
		case <-kv.shutdown:
			return
		}
	}
}


func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	close(kv.shutdown)
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int,
	gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
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
	kv.cleaningShards = make(map[int]IntSet)
	kv.historyConfigs = make([]shardmaster.Config, 0)

	kv.data = make(map[string]string)
	kv.cache = make(map[int64]string)
	kv.notifyChanMap = make(map[int]chan NotifyMsg)

	kv.pollTimer =  time.NewTimer(time.Duration(0))
	kv.pullTimer = time.NewTimer(time.Duration(0))
	kv.cleanTimer = time.NewTimer(time.Duration(0))

	go kv.run()

	return kv
}
