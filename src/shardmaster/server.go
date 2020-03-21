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
	Value		interface{} //value返回的是config，这与kvraft不同
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
	reply.Err, _ = sm.start(args.Copy())
	//DPrintf("config: %v", sm.configs)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	reply.Err, _ = sm.start(args.Copy())
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	reply.Err, _ = sm.start(args.Copy())
}

//参数args的num如果在configs范围内直接获取返回，如果不在表示最新，需要通过raft同步到各个节点然后再返回
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	sm.Lock()
	if args.Num > 0 && args.Num < len(sm.configs) {
		reply.Err, reply.Config = OK, sm.getConfig(args.Num)
		sm.Unlock()
		return
	} else {
		sm.Unlock()
		err, config := sm.start(args.Copy())
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

//添加、删除或者移动shard都会发生数据迁移
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
	//保证gids离开后，余下的gids在shards中依然均匀分布
	} else if arg, ok := msg.Command.(LeaveArgs); ok {
		if sm.cache[arg.ClientId] < arg.RequestSeq {
			newConfig := sm.getConfig(-1)
			//离开的gids集合
			leaveGIDs := make(map[int]struct{})
			for _, gid := range arg.GIDs {
				leaveGIDs[gid] = struct{}{}
				delete(newConfig.Groups, gid)
			}
			if len(newConfig.Groups) == 0 {
				newConfig.Shards = [NShards]int{}
			} else {
				//余下的gids集合
				remainingGIDs := make([]int, 0)
				for gid := range newConfig.Groups {
					remainingGIDs = append(remainingGIDs, gid)
				}
				//每个gid在shards中分布的数量
				shardsPerGID := NShards / len(remainingGIDs)
				if shardsPerGID < 1 {
					shardsPerGID = 1
				}
				// gid -> number of shards it owns
				shardsByGID := make(map[int]int)
			loop:
				//j的范围是[0,len(remainingGIDs))
				for i, j := 0, 0; i < NShards; i++ {
					gid := newConfig.Shards[i]
					if _, ok := leaveGIDs[gid]; ok || shardsByGID[gid] == shardsPerGID {
						//遍历remainingGIDs, 为i修改合适的gid
						for _, remain := range remainingGIDs {
							if shardsByGID[remain] < shardsPerGID {
								newConfig.Shards[i] = remain
								shardsByGID[remain] += 1
								continue loop
							}
						}
						//考虑到分配可能不完全均匀的情况
						//当shardsByGID所有remaining gid 数量都达到shardsPerGID 时，
						//从头开始轮询remainingGIDs加入shards和追加shardsbygid数量
						id := remainingGIDs[j]
						j = (j + 1) % len(remainingGIDs)
						newConfig.Shards[i] = id
						shardsByGID[id] += 1
					} else {
						shardsByGID[gid] += 1
					}
				}
			}

			sm.cache[arg.ClientId] = arg.RequestSeq
			sm.appendNewConfig(newConfig)
		}
	} else if arg, ok := msg.Command.(JoinArgs); ok {
		if sm.cache[arg.ClientId] < arg.RequestSeq {
			newConfig := sm.getConfig(-1)
			newGIDS := make([]int, 0)
			for gid, servers := range arg.Servers {
				if s, ok := newConfig.Groups[gid]; ok {
					newConfig.Groups[gid] = append(s, servers...)
				} else {
					newConfig.Groups[gid] = servers
					newGIDS = append(newGIDS, gid)
				}
			}
			if len(newConfig.Groups) == 0 {
				newConfig.Shards = [NShards]int{}
			//大于NShards表示加入的gid有问题，已经超过最大显示，不考虑此情况
			} else if len(newConfig.Groups) <= NShards {
				shardsPerGID := NShards / len(newConfig.Groups)
				shardsByGID := make(map[int]int)
				for i, j := 0, 0; i < NShards; i++ {
					gid := newConfig.Shards[i]
					//gid为0表示第一次加入情况，shardsByGID[gid] == shardsPerGID表示此时gid数量已足够
					//此时加入新gid
					if gid == 0 || shardsByGID[gid] == shardsPerGID {
						id := newGIDS[j]
						newConfig.Shards[i] = newGIDS[j]
						shardsByGID[id] += 1
						j = (j + 1) % len(newGIDS)
					} else {
						//gid数量不够，递增gid数量
						shardsByGID[gid] += 1
					}

				}
			}
			sm.cache[arg.ClientId] = arg.RequestSeq
			sm.appendNewConfig(newConfig)
		}
	}
	sm.notifyIfPresent(msg.CommandIndex, result)
}

func (sm *ShardMaster) run() {
	//todo ???
	go sm.rf.Replay(1)
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
