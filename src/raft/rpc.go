package raft

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
	LeaderCommit	int
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

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Err, reply.Server = OK, rf.me

	if rf.currentTerm <= args.Term {
		//正常情况下rf.currentTerm < args.Term， 因为candidate在选举前++term
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.votedFor = -1
			rf.state = Follower
		}
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			lastLogIndex := rf.logIndex - 1
			lastLogTerm := rf.getEntry(lastLogIndex).Term
			if lastLogTerm < args.LastLogTerm ||
				(lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
				rf.votedFor = args.CandidateId
				rf.state = Follower
				rf.resetElectionTimer(generateRandDuration(electionTimeout))

				reply.VoteGranted = true
				reply.Term = rf.currentTerm
				rf.persist()
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

	rf.resetElectionTimer(generateRandDuration(electionTimeout))
	//DPrintf("%v reset election timer to %v", rf.me, electionTime)
	rf.currentLeader = args.LeaderId
	rf.state = Follower
	rf.currentTerm = args.Term
	rf.votedFor = -1

	logIndex := rf.logIndex
	prevLogIndex := args.PrevLogIndex

	//条件成立说明快照已更新，而logIndex没有更新
	if prevLogIndex < rf.lastIncludedIndex {
		//DPrintf("return false, prevLogIndex: %v, lastIncludedIndex: %v", prevLogIndex, rf.lastIncludedIndex)
		reply.Success, reply.ConflictIndex = false, rf.lastIncludedIndex + 1
		return
	}

	//consistency check
	//正常情况下logIndex = prevLogIndex + 1,即两个server日志相同，当follower日志少于leader时， logIndex <= prevLogIndex
	if prevLogIndex >= logIndex || rf.getEntry(prevLogIndex).Term != args.PrevLogTerm {
		//当follower日志少于leader时， conflictIndex为rf.LogIndex - 1
		//当follower日志大于leader时， conflictIndex为prevLogIndex
		//conflictIndex总是<=args.PrevLogIndex
		conflictIndex := Min(logIndex - 1, prevLogIndex)
		conflictTerm := rf.getEntry(conflictIndex).Term
		//todo 什么情况下rf.lastIncludedIndex > rf.commitIndex
		floor := Max(rf.commitIndex, rf.lastIncludedIndex)
		for ; conflictTerm > floor && rf.getEntry(conflictIndex-1).Term == conflictTerm; conflictIndex-- {
		}
		DPrintf("prevLogIndex: %v, logIndex: %v, rf.getEntry(prevLogIndex).Term: %v," +
			"args.PrevLogTerm: %v, conflictIndex: %v", prevLogIndex, logIndex, rf.getEntry(prevLogIndex).Term,
			args.PrevLogTerm, conflictIndex)
		reply.Success, reply.Term, reply.ConflictIndex = false, args.Term, conflictIndex
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
		if rf.getEntry(prevLogIndex + 1 + i).Term != args.Entries[i].Term {
			rf.logIndex = prevLogIndex + 1 + i
			truncationIndex := rf.getOffsetIndex(rf.logIndex)
			rf.Log = rf.Log[:truncationIndex]
			break
		}
	}
	for ; i < len(args.Entries); i++ {
		rf.Log = append(rf.Log, args.Entries[i])
		rf.logIndex += 1
	}

	oldCommitIndex := rf.commitIndex
	rf.commitIndex = Min(args.LeaderCommit, rf.logIndex - 1)
	if oldCommitIndex < rf.commitIndex {
		rf.notifyApplyMsg <- struct{}{}
	}
	rf.persist()
}


func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.currentLeader = args.LeaderId
	if rf.lastIncludedIndex < args.LastIncludedIndex {
		truncationStartIndex := rf.getOffsetIndex(args.LastIncludedIndex)
		rf.lastIncludedIndex = args.LastIncludedIndex
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = rf.lastIncludedIndex
		rf.logIndex = rf.lastIncludedIndex + 1
		//正常情况下truncationStartIndex 应该>=len(rf.log),小于则说明truncationStartIndex以后的日志都是最近加入的
		//DPrintf("InstallSnapshot. before log: %v", rf.Log)
		if truncationStartIndex < len(rf.Log) {
			rf.Log = rf.Log[truncationStartIndex:]
		} else {
			rf.Log = []LogEntry{{0, nil, 0}}
		}
		//DPrintf("InstallSnapshot. after log: %v", rf.Log)
		rf.persister.SaveStateAndSnapshot(rf.getPersistState(), args.Data)
		if oldCommitIndex < rf.commitIndex {
			rf.notifyApplyMsg <- struct{}{}
		}
	}
	rf.resetElectionTimer(generateRandDuration(electionTimeout))
	rf.persist()
}