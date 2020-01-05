package raft

import (
	"bytes"
	"labgob"
	"log"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

const broadcastTime = time.Duration(100 * time.Millisecond)
const electionTimeout = time.Duration(1000 * time.Millisecond)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	LogIndex	int
	Command 	interface{}
	Term 		int
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

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	currentLeader 		int
	state 				serverState
	currentTerm			int
	logIndex			int //index of next log entry to be stored, initialized to 1
	votedFor 			int
	Log 				[]LogEntry

	commitIndex			int
	lastApplied			int
	lastIncludedIndex 	int //snapshot最后的index， 初始化为0

	//每次选举成功nextIndex都重新初始化为logIndex，所以Leader的nextIndex总是>=follower的logIndex
	nextIndex		[]int
	matchIndex		[]int
	notifyApplyMsg	chan struct{} //更新commitIndex时chan写入
	shutdown		chan struct{}
	electionTimer	*time.Timer
	testFlag		bool
}

func generateRandDuration(minDuration time.Duration) time.Duration {
	extra := time.Duration(rand.Int63()) % minDuration
	return time.Duration(minDuration + extra)
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) resetElectionTimer(duration time.Duration) {
	//if !rf.electionTimer.Stop() {
	//	<-rf.electionTimer.C
	//}
	//rf.electionTimer.Reset(duration)
	//todo 这样设置是否合适?
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(duration)
}

func (rf *Raft) getPersistState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.Log)
	e.Encode(rf.logIndex)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	data := w.Bytes()
	return data
}

//dump
func (rf *Raft) persist() {
	data := rf.getPersistState()
	rf.persister.SaveRaftState(data)
}

//load
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	currentTerm, votedFor, logIndex, commitIndex, lastApplied := 0, 0, 0, 0, 0
	//n1, n2, n3, n4, n5, n6 := d.Decode(&currentTerm), d.Decode(&votedFor), d.Decode(&logIndex),
	//	d.Decode(&commitIndex), d.Decode(&lastApplied), d.Decode(&rf.log)
	//DPrintf("%v, %v, %v, %v, %v, %v", n1, n2, n3, n4, n5, n6)
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&rf.Log) != nil ||
		d.Decode(&logIndex) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastApplied) != nil {
		//DPrintf("%v, %v, %v, %v, %v, %v", currentTerm, votedFor, logIndex, commitIndex, lastApplied, rf.log)
		log.Fatal("error while unmarshal raft state.")
	}
	rf.currentTerm, rf.votedFor, rf.logIndex, rf.commitIndex, rf.lastApplied =
		currentTerm, votedFor, logIndex, commitIndex, lastApplied
}

//lastIncludedIndex用ApplyMsg 里的CommandIndex更新,总是位于snapshot后的第一个index
func (rf *Raft) PersistAndSaveSnapshot(lastCommandIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastCommandIndex > rf.lastIncludedIndex {
		//truncationStartIndex总是等于快照log的长度
		truncationStartIndex := rf.getOffsetIndex(lastCommandIndex)
		//todo 保留truncationStartIndex是为consistency check考虑？
		rf.Log = append([]LogEntry{{0, nil, 0}}, rf.Log[truncationStartIndex:]...)
		rf.lastIncludedIndex = lastCommandIndex
		state := rf.getPersistState()
		rf.persister.SaveStateAndSnapshot(state, snapshot)
	}
}

//snapshot后lastIncludedIndex为log的起始
func (rf *Raft) getOffsetIndex(index int) int{
	return index - rf.lastIncludedIndex
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	DPrintf("call Raft start1")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("follower rf: %v", *rf)
	if rf.state != Leader {
		DPrintf("raft: sorry, %v is not leader", rf.me)
		return -1, -1, false
	}

	DPrintf("call Raft start2")
	rf.testFlag = true
	index := rf.logIndex
	term := rf.currentTerm
	entry := LogEntry{LogIndex:index, Term:term, Command:command}
	rf.Log = append(rf.Log, entry)
	rf.matchIndex[rf.me] = index
	rf.logIndex += 1
	rf.persist()
	DPrintf("currentLeader: %v, me: %v", rf.currentLeader, rf.me)
	go rf.replicate()
	DPrintf("call Raft start4")

	return index, term, true
}

func (rf *Raft) Kill() {
	//todo
}

func (rf *Raft) solicit(i int, args *RequestVoteArgs, replyCh chan<- RequestVoteReply) {
	reply := RequestVoteReply{}
	if rf.peers[i].Call("Raft.RequestVote", args, &reply) == false {
		reply.Err = RPCFAIL
		reply.Server = i
	}
	replyCh <- reply
}

func (rf *Raft) reinitIndex() {
	peersNum := len(rf.peers)
	rf.nextIndex, rf.matchIndex = make([]int, peersNum), make([]int, peersNum)
	for i := 0; i < peersNum; i++ {
		if rf.logIndex == 0 {
			DPrintf("find 0")
		}
		rf.nextIndex[i] = rf.logIndex
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) stepDown(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	rf.persist()
	rf.resetElectionTimer(generateRandDuration(electionTimeout))
}

func (rf *Raft) tick() {
	timer := time.NewTimer(broadcastTime)
	for {
		select {
		case <-timer.C:
			if _, isLeader := rf.GetState(); !isLeader {
				return
			}
			go rf.replicate()
			timer.Stop()
			timer.Reset(broadcastTime)
		case <-rf.shutdown:
			return
		}
	}
}

func (rf *Raft) replicate() {
	for follower := 0; follower < len(rf.peers); follower++ {
		if follower != rf.me {
			go rf.sendLogEntry(follower)
		}
	}
}

func (rf *Raft) sendSnapshot(follower int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	args := InstallSnapshotArgs{Term:rf.currentTerm, LeaderId:rf.me, LastIncludedIndex:rf.lastIncludedIndex,
		LastIncludedTerm:rf.Log[rf.lastIncludedIndex].Term, Data:rf.persister.ReadSnapshot()}
	rf.mu.Unlock()
	var reply InstallSnapshotReply
	//不考虑失败情况
	if rf.peers[follower].Call("Raft.InstallSnapshot", &args, &reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.stepDown(reply.Term)
		} else {
			rf.nextIndex[follower] = rf.lastIncludedIndex + 1
			rf.matchIndex[follower] = rf.lastIncludedIndex
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendLogEntry(follower int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	//follower的nextIndex已经leader被丢弃，正常情况下不会发生，网络异常或新节点加入才会发生
	if rf.nextIndex[follower] <= rf.lastIncludedIndex {
		go rf.sendSnapshot(follower)
		//todo 不发送log了？
		rf.mu.Unlock()
		return
	}

	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	prevLogIndex := rf.nextIndex[follower] - 1
	//DPrintf("follower: %v, rf.nextIndex: %v, prevLogIndex: %v, loglen: %v",
	//	follower, rf.nextIndex, prevLogIndex, len(rf.log))
	args.PrevLogIndex = prevLogIndex
	args.PrevLogTerm = rf.Log[prevLogIndex].Term
	args.LeaderCommit = rf.commitIndex
	if rf.logIndex > rf.nextIndex[follower] {
		entries := rf.Log[prevLogIndex+1:rf.logIndex]
		args.Entries = entries
	} else {
		args.Entries = nil
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	//不用理会失败情况，follower接收不到AppendEntries超时会发起选举
	if rf.peers[follower].Call("Raft.AppendEntries", &args, &reply) {
		rf.mu.Lock()
		if !reply.Success {
			if reply.Term > rf.currentTerm {
				rf.stepDown(reply.Term)
			} else {
				//retry
				rf.nextIndex[follower] = Max(1, reply.ConflictIndex)
				//DPrintf("retry rf.nextIndex: %v follower: %v confilict: %v",
				//	rf.nextIndex, follower, reply.ConflictIndex)
				go rf.sendLogEntry(follower)
				//更新nextIndex，需要判断是否需要发送snapshot rpc
				if rf.nextIndex[follower] <= rf.lastIncludedIndex {
					go rf.sendSnapshot(follower)
				}
			}
		} else {
			entriesLen := 0
			if args.Entries != nil {
				entriesLen = len(args.Entries)
			}
			commitIndex := prevLogIndex + entriesLen
			rf.nextIndex[follower] = commitIndex + 1
			rf.matchIndex[follower] = commitIndex

			//必须要majority之后才能提交
			if rf.canCommit(commitIndex) {
				rf.commitIndex = commitIndex
				rf.persist()
				rf.notifyApplyMsg <- struct{}{}
			}
		}
		rf.mu.Unlock()
	}
	//DPrintf("me: %v, leader: %v", rf.me, rf.currentLeader)
}

func (rf *Raft) canCommit(index int) bool {
	if index < rf.logIndex && index > rf.commitIndex && rf.Log[index].Term == rf.currentTerm {
		majorities, count := len(rf.peers) / 2 + 1, 0
		for i := 0; i < len(rf.peers); i++ {
			if rf.matchIndex[i] >= index {
				count += 1
			}
		}
		return count >= majorities
	} else {
		return false
	}
}

func (rf *Raft) campaign() {
	//DPrintf("%v start campaign", rf.me)
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}

	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	//generateRandDuration(electionTimeout) always greater than electionTimeout
	timer := time.After(electionTimeout)
	//DPrintf("%v start campaign. current term: %v", rf.me, rf.currentTerm)

	rf.resetElectionTimer(generateRandDuration(electionTimeout))
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.logIndex - 1
	args.LastLogTerm = rf.Log[args.LastLogIndex].Term
	rf.persist()
	rf.mu.Unlock()

	replyCh := make(chan RequestVoteReply, len(rf.peers) - 1)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.solicit(i, &args, replyCh)
		}
	}

	voteCount, majorityCount := 0, len(rf.peers)/2
	for voteCount < majorityCount {
		select {
		case <-timer:
			return
		case reply := <-replyCh:
			if reply.VoteGranted {
				voteCount += 1
			} else if reply.Err != OK {
				// retry
				go rf.solicit(reply.Server, &args, replyCh)
			} else {
				rf.mu.Lock()
				//出现network partition，产生了新的term
				if reply.Term > rf.currentTerm {
					//发现term不是最新的，stepdown后停止竞选
					rf.stepDown(reply.Term)
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}
		}
	}
	//DPrintf("server %d win the election, current term: %v", rf.me, rf.currentTerm)

	rf.mu.Lock()
	if rf.state == Candidate {
		rf.state = Leader
		rf.currentLeader = rf.me
		rf.reinitIndex()
		go rf.tick()
	}
	rf.mu.Unlock()
}

func (rf *Raft) apply(applyCh chan<- ApplyMsg) {
	for {
		select {
		case <-rf.notifyApplyMsg:
			//DPrintf("received notifyApplyMsg")
			rf.mu.Lock()
			var entries []LogEntry
			if rf.lastApplied < rf.commitIndex {
				entries = rf.Log[rf.lastApplied+1:rf.commitIndex+1]
				rf.lastApplied = rf.commitIndex
			}
			rf.persist()
			rf.mu.Unlock()

			//DPrintf("entries: %v", entries)
			for _, entry := range entries {
				//DPrintf("%v apply msg %v to applyCh", rf.me, entry)
				applyCh <- ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: entry.LogIndex}
			}
		case <-rf.shutdown:
			return
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentLeader = -1
	rf.state = Follower
	rf.currentTerm = 0
	rf.logIndex = 1
	rf.votedFor = -1
	rf.Log = []LogEntry{{0, nil, 0}} //log entry at index 0 is unused
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludedIndex = 0
	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)
	rf.notifyApplyMsg = make(chan struct{}, 100)
	rf.shutdown = make(chan struct{})
	rf.testFlag = false
	rf.electionTimer = time.NewTimer(generateRandDuration(electionTimeout))

	rf.readPersist(persister.ReadRaftState())

	//将更新后的command应用到server， 即发送到applych
	go rf.apply(applyCh)
	go func() {
		for {
			select {
			case <-rf.electionTimer.C:
				//DPrintf("%v start campaign", rf.me)
				rf.campaign()
			case <-rf.shutdown:
				return
			}
		}
	}()

	return rf
}
