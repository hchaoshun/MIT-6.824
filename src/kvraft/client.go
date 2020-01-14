package raftkv

import (
	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

const RetryInterval = time.Duration(100 * time.Millisecond)

type Clerk struct {
	servers 		[]*labrpc.ClientEnd
	clientId		int64
	requestSeq		int
	leaderId		int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.requestSeq = 0
	ck.leaderId = 0
	return ck
}

func (ck *Clerk) Get(key string) string {
	for {
		args := GetArgs{Key:key}
		var reply GetReply
		if ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply) &&
			reply.Err == "OK" {
			return reply.Value
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
		//DPrintf("Get args: %v, return err. retry", args)
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.requestSeq++
	for {
		args := PutAppendArgs{Key:key, Value:value, Op:op, ClientId:ck.clientId, RequestSeq:ck.requestSeq}
		var reply PutAppendReply
		if ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply) &&
			reply.Err == "OK" {
			return
		}

		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
		//DPrintf("PutAppend args: %v, return err. retry", args)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
