package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func Min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	//注意：必须带term，可能由于partition，发送到管道的term可能大于start后收到的term
	CommandTerm	 int
	CommandIndex int
}

type LogEntry struct {
	LogIndex	int
	Command 	interface{}
	LogTerm  	int
}

type serverState int
const (
	Follower serverState = iota
	Candidate
	Leader
)

type Err string
const (
	OK = 	"OK"
	RPCFAIL =	"RPCFAIL"
)

type RequestVoteArgs struct {
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

type RequestVoteReply struct {
	Term 			int
	VoteGranted		bool
	//下面两个变量用于追踪错误原因和错误机器
	Server 			int // which peer
	Err 			Err
}

type AppendEntriesArgs struct {
	Term 			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries 		[]LogEntry
	CommitIndex		int //此参数更新follower的commitIndex，必要时apply
	//LeaderCommit		int
	Len 			int
}

type AppendEntriesReply struct {
	Term 			int
	Success			bool
	ConflictIndex	int
}

type InstallSnapshotArgs struct {
	Term 				int
	LeaderId			int
	LastIncludedIndex	int
	LastIncludedTerm 	int
	Data 				[]byte
}

type InstallSnapshotReply struct {
	Term 				int
}