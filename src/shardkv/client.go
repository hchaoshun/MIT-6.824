package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's Shard.
//

import "labrpc"
import "crypto/rand"
import "math/big"
import "shardmaster"
import "time"

//
// which Shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

//client 和 server 都存储最近的config，client发送到server时需要比较config里的num是否一致
type Clerk struct {
	sm       			*shardmaster.Clerk //shardmaster 的client端
	config   			shardmaster.Config
	make_end 			func(string) *labrpc.ClientEnd
	clientId			int64
	lastRequestId		int64
}

//初始化client的时候需要和shardmaster通信获取最新的配置
//config用于确定发出的key属于哪一个group
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	ck.clientId = nrand()
	ck.lastRequestId = 0
	ck.config = ck.sm.Query(-1)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		args.ConfigNum = ck.config.Num
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the Shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				//发生此错误说明config不是最新的，需要获取最新config再重试
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	//不同client可能会在同一时间发出请求，单独的时间戳无法保证id的唯一
	requestId := time.Now().UnixNano() - ck.clientId
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op
	args.ConfigNum = ck.config.Num
	args.RequestId = requestId
	args.ExpireRequestId = ck.lastRequestId

	ck.lastRequestId = requestId

	for {
		DPrintf("client config: %v", ck.config)
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		//注意：第二次循环以后config更新，confignum也随着更新
		args.ConfigNum = ck.config.Num
		DPrintf("client args.ConfigNum: %v", args.ConfigNum)
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				args.ConfigNum = ck.config.Num
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		DPrintf("client wrong group, try again.")
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
