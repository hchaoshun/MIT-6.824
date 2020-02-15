package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers 	[]*labrpc.ClientEnd
	clientId	int64
	requestSeq	int
	leaderId	int
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

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{Num:num}
	for {
		var reply QueryReply
		if ck.servers[ck.leaderId].Call("ShardMaster.Query", &args, &reply) &&
			reply.WrongLeader == false {
			return reply.Config
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.requestSeq++
	args := JoinArgs{clientId:ck.clientId, requestSeq:ck.requestSeq, Servers:servers}
	for {
		var reply JoinReply
		if ck.servers[ck.leaderId].Call("ShardMaster.Join", &args, &reply) &&
			reply.WrongLeader == false {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.requestSeq++
	args := LeaveArgs{clientId:ck.clientId, requestSeq:ck.requestSeq, GIDs:gids}
	for {
		var reply LeaveReply
		if ck.servers[ck.leaderId].Call("ShardMaster.Leave", &args, &reply) &&
			reply.WrongLeader == false {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.requestSeq++
	args := MoveArgs{clientId:ck.clientId, requestSeq:ck.requestSeq, Shard:shard, GID:gid}
	for {
		var reply MoveReply
		if ck.servers[ck.leaderId].Call("ShardMaster.Move", &args, &reply) &&
			reply.WrongLeader == false {
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
