package raft

import (
	"fmt"
)

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Err = OK
	reply.Server = rf.me
	rf.printLog()
	if rf.currentTerm > args.Term { // 当前server term更大，直接返回false
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	if rf.currentTerm == args.Term && rf.votedFor != -1 { // 如果term相等, 且已经投过票, 返回false
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		if rf.state != Follower { // 需要降级
			rf.state = Follower
			rf.electionTimer.Stop()
			rf.electionTimer.Reset(generateRandDuration(ElectiontTimeout))
		}
	}
	rf.leaderId = -1
	lastLogIndex := rf.log[rf.getLastLogIndex()].LogIndex
	lastLogTerm := rf.log[rf.getLastLogIndex()].LogTerm
	if lastLogTerm > args.LastLogTerm ||
		(lastLogTerm == args.LastLogTerm && args.LastLogIndex < lastLogIndex) {
		reply.VoteGranted = false
		reply.Term = args.Term
		return
	}
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(generateRandDuration(ElectiontTimeout))
	rf.persist()
	return
}


// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}
	if rf.leaderId == -1 || rf.leaderId != args.LeaderId {
		rf.leaderId = args.LeaderId
	}
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(generateRandDuration(ElectiontTimeout))
	rf.state, rf.votedFor = Follower, -1
	prevLogIndex := args.PrevLogIndex
	if prevLogIndex < rf.LastIncludedIndex {
		reply.Success, reply.ConflictIndex = false, rf.LastIncludedIndex+1
		return
	}
	lastLogIndex := rf.log[rf.getLastLogIndex()].LogIndex
	if lastLogIndex < prevLogIndex || rf.log[prevLogIndex - rf.LastIncludedIndex].LogTerm != args.PrevLogTerm {
		reply.Success = false
		conflictIndex := Min(lastLogIndex, prevLogIndex)
		conflictTerm := rf.log[conflictIndex - rf.LastIncludedIndex].LogTerm
		upper := Max(rf.LastIncludedIndex, rf.commitIndex)
		for ; conflictIndex > upper && rf.log[conflictIndex- 1 - rf.LastIncludedIndex].LogTerm == conflictTerm ; conflictIndex -- {
		}
		reply.ConflictIndex = conflictIndex
		return
	}
	reply.Success = true
	reply.ConflictIndex = -1
	i := 0
	for ; i < args.Len; i++ {
		if prevLogIndex+1+i > lastLogIndex {
			break
		}
		// 这里的判断对应LogMatch约束，
		if rf.log[prevLogIndex + 1 + i - rf.LastIncludedIndex].LogTerm != args.Entries[i].LogTerm { // 如果从某个index开始term冲突，保留之前的，删除之后的
			lastLogIndex = prevLogIndex + i
			rf.log = append(rf.log[:lastLogIndex + 1 - rf.LastIncludedIndex]) // delete any conflicting log entries
			break
		}
	}
	for ; i < args.Len; i++ {
		rf.log = append(rf.log, args.Entries[i])
	}
	oldCommitIndex := rf.commitIndex
	//follower 根据 leader的请求，知道leader的commitIndex，再更新自己的commitIndex
	rf.commitIndex = Max(rf.commitIndex, Min(args.LeaderCommit, args.PrevLogIndex+args.Len)) //
	rf.persist()
	if rf.commitIndex > oldCommitIndex {
		// apply
		fmt.Printf("server %d send a notifyApply, commitIndex= %d   \n", rf.me, rf.commitIndex)

		rf.notifyApply <- struct{}{}
	}
}

func (rf * Raft) InstallSnapshot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	rf.leaderId = args.LeaderId
	if args.LastIncludedIndex > rf.LastIncludedIndex {
		truncation := args.LastIncludedIndex - rf.LastIncludedIndex
		rf.LastIncludedIndex = args.LastIncludedIndex
		oldCommitIndex := rf.commitIndex
		rf.commitIndex = Max(rf.commitIndex, rf.LastIncludedIndex)
		if truncation < len(rf.log) { // 截断
			rf.log = append(rf.log[truncation:]) //多保留一个，相当于nil
		} else { //全部丢弃
			rf.log = []LogEntry{{args.LastIncludedIndex, args.LastIncludedTerm, nil}}
		}
		rf.persister.SaveStateAndSnapshot(rf.getPersistState(), args.Data) //replace service state with snapshot contents
		if rf.commitIndex > oldCommitIndex {
			rf.notifyApply <- struct{}{}
		}
	}
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(generateRandDuration(ElectiontTimeout))
	rf.persist()
	fmt.Printf("server %d installSnapshot success \n", rf.me)
}