package shardmaster

//
// Shardmaster clerk.
//

import "labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	Servers 	[]*labrpc.ClientEnd
	ClientId	int64
	RequestSeq	int
	LeaderId	int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.Servers = servers
	ck.ClientId = nrand()
	ck.RequestSeq = 0
	ck.LeaderId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{Num:num}
	for {
		var reply QueryReply
		if ck.Servers[ck.LeaderId].Call("ShardMaster.Query", &args, &reply) &&
			reply.Err == OK {
			return reply.Config
		}
		ck.LeaderId = (ck.LeaderId + 1) % len(ck.Servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.RequestSeq++
	args := JoinArgs{ClientId:ck.ClientId, RequestSeq:ck.RequestSeq, Servers:servers}
	for {
		var reply JoinReply
		if ck.Servers[ck.LeaderId].Call("ShardMaster.Join", &args, &reply) &&
			reply.Err == OK {
			return
		}
		ck.LeaderId = (ck.LeaderId + 1) % len(ck.Servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.RequestSeq++
	args := LeaveArgs{ClientId:ck.ClientId, RequestSeq:ck.RequestSeq, GIDs:gids}
	for {
		var reply LeaveReply
		if ck.Servers[ck.LeaderId].Call("ShardMaster.Leave", &args, &reply) &&
			reply.Err == OK {
			return
		}
		ck.LeaderId = (ck.LeaderId + 1) % len(ck.Servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.RequestSeq++
	args := MoveArgs{ClientId:ck.ClientId, RequestSeq:ck.RequestSeq, Shard:shard, GID:gid}
	for {
		var reply MoveReply
		if ck.Servers[ck.LeaderId].Call("ShardMaster.Move", &args, &reply) &&
			reply.Err == OK {
			return
		}
		ck.LeaderId = (ck.LeaderId + 1) % len(ck.Servers)
		time.Sleep(100 * time.Millisecond)
	}
}
