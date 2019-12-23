package raft

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

type RequestVoteReply struct {
	Term 			int
	VoteGranted		bool
	Server 			int // which peer
	Err 			Err
}

type AppendEntriesArgs struct {
	Term 			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	Entries 		[]LogEntry
	LeaderCommit	int
}

type AppendEntriesReply struct {
	Term 			int
	Success			bool
	ConflictIndex	int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Err, reply.Server = OK, rf.me

	if rf.currentTerm <= args.Term {
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.state = Follower
		}
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			lastLogIndex := rf.logIndex - 1
			lastLogTerm := rf.log[lastLogIndex].Term
			if lastLogTerm < args.LastLogTerm ||
				(lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
				rf.votedFor = args.CandidateId
				rf.state = Follower
				rf.resetElectionTimer(generateRandDuration(electionTimeout))

				reply.VoteGranted = true
				reply.Term = rf.currentTerm
				return
			}
		}
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	electionTime := generateRandDuration(electionTimeout)
	rf.resetElectionTimer(electionTime)
	//DPrintf("%v reset election timer to %v", rf.me, electionTime)
	rf.currentLeader = args.LeaderId
	rf.state = Follower
	rf.currentTerm = args.Term
	rf.votedFor = -1

	logIndex := rf.logIndex
	prevLogIndex := args.PrevLogIndex
	//consistency check
	if prevLogIndex >= logIndex || rf.log[prevLogIndex].Term != args.PrevLogTerm {
		reply.Success, reply.Term, reply.ConflictIndex = false, args.Term, Min(logIndex - 1, prevLogIndex)
		return
	}

	reply.Success, reply.ConflictIndex = true, -1
	var i = 0
	//补全缺失或错误的log
	//正常情况下for只会执行一次
	for ; i < len(args.Entries); i++ {
		//代表缺失情况，找到缺失位置跳出
		if prevLogIndex + 1 + i >= logIndex {
			break
		}
		//代表不匹配情况，找到不匹配位置删除然后跳出
		if rf.log[prevLogIndex + 1 + i].Term != args.Entries[i].Term {
			rf.logIndex = prevLogIndex + 1 + i
			rf.log = rf.log[:rf.logIndex]
			break
		}
	}
	for ; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i])
		rf.logIndex += 1
	}

	oldCommitIndex := rf.commitIndex
	rf.commitIndex = Min(args.LeaderCommit, rf.logIndex - 1)
	if oldCommitIndex < rf.commitIndex {
		rf.notifyApplyMsg <- struct{}{}
	}
}
