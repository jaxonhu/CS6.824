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
	fmt.Printf("server %d receive RequestVote RPC from %d, argsTerm= %d, args.LastLogTerm= %d args.LastLogIndex= %d currentTerm= %d\n", rf.me, args.CandidateId, args.Term, args.LastLogTerm, args.LastLogIndex, rf.currentTerm)
	reply.Err = OK
	reply.Server = rf.me
	rf.printLog()
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
		fmt.Printf("4 follower %d vote false, currentTerm = %d, voted = %d, currentTime= %v \n", rf.me, rf.currentTerm, rf.votedFor, time.Now().UnixNano() / 1e6)
		return
	}
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(generateRandDuration(ElectiontTimeout))
	rf.persist()
	fmt.Printf("follower %d vote success, leaderId= %d currentTerm = %d voted = %d, currentTime= %v \n", rf.me, rf.leaderId, rf.currentTerm, rf.votedFor, time.Now().UnixNano() / 1e6)
	return
}


// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("server %d receive AppendEntries RPC from %d, argsTerm= %d, argsPrevLogTerm= %d currentTerm= %d, currentTime= %v \n", rf.me, args.LeaderId, args.Term, args.PrevLogTerm, rf.currentTerm, time.Now().UnixNano() / 1e6)
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.Success = false
		fmt.Printf("server %d reply false caused by term, argsTerm= %d, serverTerm= %d currentTime= %v \n",
			rf.me, args.Term, rf.currentTerm, time.Now().UnixNano() / 1e6)
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
	if prevLogIndex < rf.LastIncludedIndex {
		fmt.Printf("server %d reply false caused by LastIncludedIndex prevLogIndex= %d, rf.LastIncludedIndex= %d \n", rf.me, prevLogIndex, rf.LastIncludedIndex)
		reply.Success, reply.ConflictIndex = false, rf.LastIncludedIndex+1
		return
	}
	lastLogIndex := rf.log[rf.getLastLogIndex()].LogIndex
	if lastLogIndex < prevLogIndex || rf.log[prevLogIndex - rf.LastIncludedIndex].LogTerm != args.PrevLogTerm {
		reply.Success = false
		if lastLogIndex >= prevLogIndex {
			fmt.Printf("follower %d reply false caused by inconsistent args.PrevLogIndex= %d lastLogIndex= %d log[prev].Term= %d currentTime= %d\n", rf.me, args.PrevLogIndex, lastLogIndex, rf.log[prevLogIndex - rf.LastIncludedIndex].LogTerm, time.Now().UnixNano()/1e6)
		} else {
			fmt.Printf("follower %d reply false caused by lag args.PrevLogIndex= %d lastLogIndex= %d currentTime= %d\n", rf.me, args.PrevLogIndex, lastLogIndex, time.Now().UnixNano()/1e6)
		}
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
	fmt.Printf("follower %d reply true args.PrevLogIndex= %d lastLogIndex= %d leaderCommit= %d commitIndex= %d currentTime= %d \n", rf.me, args.PrevLogIndex, lastLogIndex, args.LeaderCommit, rf.commitIndex, time.Now().UnixNano() / 1e6)
	i := 0
	for ; i < args.Len; i++ {
		if prevLogIndex+1+i > lastLogIndex {
			break
		}
		if rf.log[prevLogIndex + 1 + i - rf.LastIncludedIndex].LogTerm != args.Entries[i].LogTerm { // 如果从某个index开始term冲突，保留之前的，删除之后的
			lastLogIndex = prevLogIndex + i
			rf.log = append(rf.log[:lastLogIndex + 1 - rf.LastIncludedIndex]) // delete any conflicting log entries
			break
		}
	}
	for ; i < args.Len; i++ {
		fmt.Printf("server %d append entry to its log, logTerm= %d logIndex= %d  \n", rf.me,  args.Entries[i].LogTerm, args.Entries[i].LogIndex)
		rf.log = append(rf.log, args.Entries[i])
	}
	oldCommitIndex := rf.commitIndex
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
	fmt.Printf("server %d invoke snapshot args.LastIncludedIndex= %d rf.LastIncludedIndex= %d \n", rf.me, args.LastIncludedIndex, rf.LastIncludedIndex)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		fmt.Printf("server %d installSnapshot fail cause of term args.Term= %d currentTerm= %d", rf.me, args.Term, rf.currentTerm)
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