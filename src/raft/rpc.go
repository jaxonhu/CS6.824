package raft

import (
	"fmt"
	"time"
)

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("server %d receive RequestVote RPC from %d, argsTerm= %d, currentTerm= %d\n", rf.me, args.CandidateId, args.Term, rf.currentTerm)
	reply.Err = OK
	reply.Server = rf.me
	if rf.currentTerm > args.Term { // 当前server term更大，直接返回false
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		fmt.Printf("1 follower %d vote false, argsTerm = %d, currentTerm = %d, voted = %d, currentTime= %v \n", rf.me, args.Term, rf.currentTerm, rf.votedFor, time.Now().UnixNano() / 1e6)
		return
	}
	if rf.currentTerm == args.Term && rf.votedFor != -1 { // 如果term相等, 且已经投过票, 返回false
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if rf.currentTerm < args.Term {
		if rf.state != Follower { // 需要降级
			rf.state = Follower
			rf.electionTimer.Stop()
			rf.electionTimer.Reset(generateRandDuration(ElectiontTimeout))
		}
	}
	rf.leaderId = -1
	lastLogIndex := rf.logIndex - 1
	lastLogTerm := rf.log[lastLogIndex].LogTerm
	if lastLogTerm > args.Term ||
		(lastLogTerm == args.Term && args.LastLogIndex < lastLogIndex) {
		reply.VoteGranted = false
		reply.Term = args.Term
		fmt.Printf("4 follower %d vote false, currentTerm = %d, voted = %d, currentTime= %v \n", rf.me, rf.currentTerm, rf.votedFor, time.Now().UnixNano() / 1e6)
		return
	}
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(generateRandDuration(ElectiontTimeout))
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	fmt.Printf("follower %d vote success, leaderId= %d currentTerm = %d voted = %d, currentTime= %v \n", rf.me, rf.leaderId, rf.currentTerm, rf.votedFor, time.Now().UnixNano() / 1e6)
	return
}


// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("server %d receive AppendEntries RPC from %d, argsTerm= %d, currentTerm= %d, currentTime= %v \n", rf.me, args.LeaderId, args.Term, rf.currentTerm, time.Now().UnixNano() / 1e6)
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		fmt.Printf("server %d reply false caused by term, argsTerm= %d, serverTerm= %d\n",
			rf.me, args.Term, rf.currentTerm)
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		fmt.Printf("server %d update its term from %d to %d\n", rf.me, args.Term, rf.currentTerm)
	}
	if rf.leaderId == -1 || rf.leaderId != args.LeaderId {
		rf.leaderId = args.LeaderId
	}
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(generateRandDuration(ElectiontTimeout))
	rf.state, rf.votedFor = Follower, -1
	prevLogIndex := args.PrevLogIndex

	if rf.logIndex <= prevLogIndex || rf.log[prevLogIndex].LogTerm != args.PrevLogTerm {
		conflictIndex := Min(rf.logIndex - 1, prevLogIndex)
		conflictTerm := rf.log[conflictIndex].LogTerm
		for ; conflictIndex > rf.commitIndex && rf.log[conflictIndex - 1].LogTerm == conflictTerm ; conflictIndex -- {
		}
		reply.Success = false
		reply.ConflictIndex = conflictIndex
		fmt.Printf("follower %d reply false caused by inconsistent\n", rf.me)
		return
	}
	reply.Success = true
	reply.ConflictIndex = -1
	i := 0
	for ; i < args.Len; i++ {
		if prevLogIndex+1+i >= rf.logIndex {
			break
		}
		if rf.log[prevLogIndex + 1 + i].LogTerm != args.Entries[i].LogTerm {
			rf.logIndex = prevLogIndex + 1 + i
			rf.log = append(rf.log[:rf.logIndex]) // delete any conflicting log entries
			break
		}
	}
	for ; i < args.Len; i++ {
		rf.log = append(rf.log, args.Entries[i])
		rf.logIndex += 1
	}
	oldCommitIndex := rf.commitIndex
	rf.commitIndex = Max(rf.commitIndex, Min(args.LeaderCommit, args.PrevLogIndex+args.Len))
	if rf.commitIndex > oldCommitIndex {
		// apply
	}
}