package raft

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
			lastLogTerm := rf.getEntry(lastLogIndex).LogTerm
			if lastLogTerm < args.LastLogTerm ||
				(lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {
				rf.votedFor = args.CandidateId
				rf.state = Follower
				rf.resetElectionTimer(newRandDuration(ElectionTimeout))

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

	if rf.TestFlag {
		DPrintf("follower %v log: %v logIndex: %v, lastIncludedIndex: %v", rf.me, rf.log,
			rf.logIndex, rf.lastIncludedIndex)
	}
	if rf.TestFlag {
		DPrintf("Term: %v, leaderId: %v, PrevLogIndex: %v, PrevLogTerm: %v, Entries: %v," +
			"LeaderCommit: %v", args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm,
			args.Entries, args.CommitIndex)
	}
	if rf.currentTerm > args.Term {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	rf.resetElectionTimer(newRandDuration(ElectionTimeout))
	//DPrintf("%v reset election timer to %v", rf.me, electionTime)
	rf.leaderId = args.LeaderId
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
	if prevLogIndex >= logIndex || rf.getEntry(prevLogIndex).LogTerm != args.PrevLogTerm {
		//当follower日志少于leader时， conflictIndex为rf.LogIndex - 1
		//当follower日志大于leader时， conflictIndex为prevLogIndex
		//conflictIndex总是<=args.PrevLogIndex
		conflictIndex := Min(logIndex - 1, prevLogIndex)
		DPrintf("AppendEntries: prevLogIndex: %v, logIndex: %v, conflictIndex: %v, lastIncludedIndex: %v",
			prevLogIndex, logIndex, conflictIndex, rf.lastIncludedIndex)
		DPrintf("AppendEntries: log: %v", rf.log)
		conflictTerm := rf.getEntry(conflictIndex).LogTerm
		//todo 什么情况下rf.lastIncludedIndex > rf.commitIndex
		floor := Max(rf.commitIndex, rf.lastIncludedIndex)
		//过滤相同的conflictTerm
		for ; conflictTerm > floor && rf.getEntry(conflictIndex-1).LogTerm == conflictTerm; conflictIndex-- {
		}
		//DPrintf("prevLogIndex: %v, logIndex: %v", prevLogIndex, logIndex)
		reply.Success, reply.Term, reply.ConflictIndex = false, args.Term, conflictIndex
		return
	}

	reply.Success, reply.ConflictIndex = true, -1
	var i = 0
	//补全缺失或错误的log
	//正常情况下for只会执行一次
	//todo for 循环能不能用if代替?
	for ; i < len(args.Entries); i++ {
		//代表缺失情况(follower日志少于leader)，找到缺失位置跳出
		if prevLogIndex + 1 + i >= logIndex {
			break
		}
		//代表不匹配情况，找到不匹配位置删除然后跳出
		if rf.getEntry(prevLogIndex + 1 + i).LogIndex != args.Entries[i].LogIndex {
			rf.logIndex = prevLogIndex + 1 + i
			truncationIndex := rf.getOffsetIndex(rf.logIndex)
			rf.log = rf.log[:truncationIndex]
			break
		}
	}
	for ; i < len(args.Entries); i++ {
		rf.log = append(rf.log, args.Entries[i])
		rf.logIndex += 1
	}

	oldCommitIndex := rf.commitIndex
	rf.commitIndex = Min(args.CommitIndex, rf.logIndex - 1)
	if oldCommitIndex < rf.commitIndex {
		rf.notifyApplyCh <- struct{}{}
	}
	rf.persist()
}

//func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	reply.Err = OK
//	reply.Term = rf.currentTerm
//	if args.Term < rf.currentTerm {
//		DPrintf("InstallSnapshot: args.Term: %v < rf.currentTerm: %v," +
//			"return", args.Term, rf.currentTerm)
//		return
//	}
//	rf.leaderId = args.LeaderId
//	if args.LastIncludedIndex > rf.lastIncludedIndex {
//		truncationStartIndex := rf.getOffsetIndex(args.LastIncludedIndex)
//		rf.lastIncludedIndex = args.LastIncludedIndex
//		oldCommitIndex := rf.commitIndex
//		rf.commitIndex = Max(rf.commitIndex, rf.lastIncludedIndex)
//		rf.logIndex = Max(rf.logIndex, rf.lastIncludedIndex+1)
//		//DPrintf("InstallSnapshot. before log: %v", rf.log)
//		if truncationStartIndex < len(rf.log) { // snapshot contain a prefix of its log
//			rf.log = append(rf.log[truncationStartIndex:])
//		} else { // snapshot contain new information not already in the follower's log
//			rf.log = []LogEntry{{args.LastIncludedIndex, nil, args.LastIncludedTerm}} // discards entire log
//		}
//		//DPrintf("InstallSnapshot. after log: %v", rf.log)
//		rf.persister.SaveStateAndSnapshot(rf.getPersistState(), args.Data)
//		if rf.commitIndex > oldCommitIndex {
//			rf.notifyApplyCh <- struct{}{}
//		}
//	}
//	rf.resetElectionTimer(newRandDuration(ElectionTimeout))
//	rf.persist()
//}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.leaderId = args.LeaderId
	if rf.lastIncludedIndex < args.LastIncludedIndex {
		truncationStartIndex := rf.getOffsetIndex(args.LastIncludedIndex)
		rf.lastIncludedIndex = args.LastIncludedIndex
		oldCommitIndex := rf.commitIndex

		//可能会出现rf.commitIndex > rf.lastIncludedIndex 或 rf.logIndex > rf.lastIncludedIndex+1的情况
		//leader:   16, 17
		//follower: 09, 10,...,16, 17
		//此时leader: lastIncludedIndex: 16
		//此时follower: commitIndex: 16, logIndex: 18, lastIncludedIndex: 09
		//此时rf.lastIncludedIndex+1(17) < rf.logIndex(18)
		rf.commitIndex = Max(rf.commitIndex, rf.lastIncludedIndex)
		rf.logIndex = Max(rf.logIndex, rf.lastIncludedIndex+1)
		//正常情况下truncationStartIndex 应该>=len(rf.log),小于则说明truncationStartIndex以后的日志都是最近加入的
		//DPrintf("InstallSnapshot. before log: %v", rf.Log)
		if truncationStartIndex < len(rf.log) {
			rf.log = rf.log[truncationStartIndex:]
		} else {
			//todo
			//rf.Log = []LogEntry{{0, nil, 0}}
			rf.log = []LogEntry{{args.LastIncludedIndex, nil, args.LastIncludedTerm}}
		}
		//DPrintf("InstallSnapshot. after log: %v", rf.Log)
		rf.persister.SaveStateAndSnapshot(rf.getPersistState(), args.Data)
		if oldCommitIndex < rf.commitIndex {
			rf.notifyApplyCh <- struct{}{}
		}
	}
	rf.resetElectionTimer(newRandDuration(ElectionTimeout))
	rf.persist()
}
