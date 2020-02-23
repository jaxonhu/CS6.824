package raft

import "fmt"

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Err = OK
	reply.Server = rf.me
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		fmt.Printf("1 follower %d vote false, argsTerm = %d currentTerm = %d voted = %d \n", rf.me, args.Term, rf.currentTerm, rf.votedFor)
		return
	}
	if  rf.votedFor != -1 {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		fmt.Printf("2 follower %d vote false, argsTerm = %d currentTerm = %d voted = %d \n", rf.me, args.Term, rf.currentTerm, rf.votedFor)
		return
	}
	if rf.state == Candidate {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.leaderId = -1
		rf.electionTimer.Stop()
		rf.electionTimer.Reset(generateRandDuration(ElectiontTimeout))
		//reply.VoteGranted = false
		//reply.Term = rf.currentTerm
		//fmt.Printf(" 3 follower %d vote false, currentTerm = %d voted = %d \n", rf.me, rf.currentTerm, rf.votedFor)
		//return
	}
	lastLogIndex := rf.logIndex - 1
	lastLogTerm := rf.log[lastLogIndex].LogTerm
	if lastLogTerm > args.Term ||
		(lastLogTerm == args.Term && args.LastLogIndex < lastLogIndex) {
		reply.VoteGranted = false
		reply.Term = args.Term
		fmt.Printf("4 follower %d vote false, currentTerm = %d voted = %d \n", rf.me, rf.currentTerm, rf.votedFor)
		return
	}
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	fmt.Printf("follower %d vote success, currentTerm = %d voted = %d \n", rf.me, rf.currentTerm, rf.votedFor)
	return
}


// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	rf.mu.Unlock()

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