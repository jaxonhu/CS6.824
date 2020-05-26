package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"labgob"
	"log"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid 	bool
	Command      	interface{}
	CommandIndex 	int
	CommandTerm 	int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	leaderId int				  // leader's id
	currentTerm int				  // latest term server has seen, initialized to 0
	votedFor int				  // candidate that received vote in current term
	// volatile state
	commitIndex int 			  // index of highest log entry known to be committed, initialized to 0
	lastApplied int				  // index of highest log entry applied to state machine, initialized to 0
	// leader state
	state serverState 			  // state of server
	log []LogEntry 				  // log entries
	nextIndex []int				  // for each server, index of the next log entry to send to that server
	matchIndex []int              // for each server, index of highest log entry, used to track committed index
	applyChan chan ApplyMsg 	  // apply to client

	shutdown chan struct{}
	electionTimer *time.Timer
	heartbeatTimer *time.Timer
	notifyApply chan struct{}

	// for snapshot
	LastIncludedIndex int		// the snapshot replaces all entries up through and including this index
	LastIncludedTerm int

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	// Your code here (2A).
	term = rf.currentTerm
	var isLeader bool
	var output string
	if rf.state == Leader {
		output = "Leader"
	}
	if rf.state == Follower {
		output = "Follower"
	}
	if rf.state == Candidate {
		output = "Candidate"
	}
	if rf.leaderId == rf.me && rf.state == Leader {
		isLeader = true
	} else if rf.leaderId != rf.me && rf.state != Leader {
		isLeader = false
	} else {
		fmt.Printf("server %d GetState error, me= %d, leaderId= %d, state=%s \n", rf.me, rf.me, rf.leaderId, output)
	}
	return term, isLeader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	data := rf.getPersistState()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) getPersistState () []byte {
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.LastIncludedIndex)
	encoder.Encode(rf.commitIndex)
	encoder.Encode(rf.lastApplied)
	encoder.Encode(rf.log)
	data := buffer.Bytes()
	return data
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist() {
	data := rf.persister.ReadRaftState()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	currentTerm, votedFor, lastIncludedIndex, commitIndex, lastApplied := 0, 0, 0, 0, 0
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&rf.log) != nil {
		log.Fatal("Error in unmarshal raft state")
	}
	rf.currentTerm, rf.votedFor, rf.LastIncludedIndex, rf.commitIndex, rf.lastApplied = currentTerm, votedFor, lastIncludedIndex, commitIndex, lastApplied
}


func (rf *Raft) getLastLogIndex() int {
	index := len(rf.log) - 1
	if index != rf.log[index].LogIndex {
		fmt.Printf("server %d log index not match in pos %d  %d \n", rf.me, index, rf.log[index].LogIndex)
		rf.printLog()
	}
	return len(rf.log) - 1
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		fmt.Printf("server %d not leader so Start return false \n", rf.me)
		return -1, -1, false
	}
	//index := rf.getLastLogIndex()
	index := rf.log[rf.getLastLogIndex()].LogIndex
	offset := index + 1 - rf.LastIncludedIndex
	term := rf.currentTerm
	fmt.Printf("server %d get a command, logIndex= %d, LogTerm= %d \n", rf.me, index+1, term)
	entry := LogEntry{
		LogIndex: index+1,
		LogTerm:  term,
		Command:  command,
	}
	if offset < len(rf.log) {
		rf.log[offset] = entry
	} else {
		rf.log = append(rf.log, entry)
	}
	rf.matchIndex[rf.me] = index + 1
	rf.persist()
	go func() {
		for follower := 0 ; follower < len(rf.peers) ; follower ++ {
			if follower != rf.me {
				go rf.sendAppendEntries(follower)
			}
		}
	}()
	fmt.Printf("server %d Start return term= %d index= %d \n", rf.me, term, index+1)
	return index+1, term, true
}


//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = Follower
	close(rf.shutdown)
	fmt.Printf("Kill raft %d at %d commit index: %d, last applied index: %d \n",
		rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = Follower
	rf.votedFor = -1
	rf.leaderId = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.shutdown = make(chan struct{})
	rf.applyChan = applyCh
	rf.log =  []LogEntry{{0, 0, nil}} // log entry at index 0 is unused
	rf.electionTimer = time.NewTimer(generateRandDuration(ElectiontTimeout))
	rf.notifyApply = make(chan struct{}, 100)
	rf.LastIncludedIndex = 0
	rf.LastIncludedTerm = 0
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist()
	// 作为follower监听AppendEntries请求
	go rf.apply()
	go func() {
		for {
			select {
			case <- rf.electionTimer.C:
				rf.campaign()
			case <- rf.shutdown:
				return
			}
		}
	}()
	return rf
}

func (rf *Raft) apply() {
	for {
		select {
		case <- rf.notifyApply:
			rf.mu.Lock()
			var commandValid bool
			var entries []LogEntry
			lastLogIndex := rf.log[rf.getLastLogIndex()].LogIndex
			if rf.lastApplied < rf.LastIncludedIndex { // apply的落后于snapshot
				commandValid = false
				rf.lastApplied = rf.LastIncludedIndex
				entries = [] LogEntry{{LogIndex: rf.LastIncludedIndex, LogTerm: rf.log[0].LogTerm, Command: "InstallSnapshot"}} // notify included snapshot to its kvserver
			}else if rf.lastApplied <= lastLogIndex && rf.lastApplied < rf.commitIndex { // 更新本地lastApplied
				commandValid = true
				entries = rf.getRangeEntry(rf.lastApplied + 1, rf.commitIndex)
				rf.lastApplied = rf.commitIndex
			}
			rf.persist()
			rf.mu.Unlock()
			for _, entry := range entries {
				rf.applyChan <- ApplyMsg{CommandValid: commandValid, Command:entry.Command, CommandIndex: entry.LogIndex, CommandTerm: entry.LogTerm}
			}
		case <- rf.shutdown:
			return
		}
	}
}

func (rf *Raft) printLog() {
	fmt.Printf("server %d log : ", rf.me)
	//for k, v  := range rf.log {
	//	fmt.Printf("[ index= %d Command= %v CommandIndex= %d CommandTerm= %d ] ", k, v.Command, v.LogIndex, v.LogTerm)
	//}
	fmt.Println(rf.log)
	fmt.Printf("----------- \n")
}

func (rf *Raft) campaign() {
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.printLog()
	rf.state = Candidate
	rf.leaderId = -1
	rf.currentTerm += 1
	rf.votedFor = rf.me
	lastLogIndex := rf.log[rf.getLastLogIndex()].LogIndex
	lastLogTerm := rf.log[rf.getLastLogIndex()].LogTerm
	me := rf.me // 注意竞态条件
	rf.persist()
	fmt.Printf("server %d begin to vote for leader, currentTerm= %d, currentTime= %v \n", rf.me, rf.currentTerm, time.Now().UnixNano() / 1e6)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	electionDuration := generateRandDuration(ElectiontTimeout)
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(electionDuration)
	timer := time.After(electionDuration)
	rf.mu.Unlock()
	replyChan := make(chan RequestVoteReply, len(rf.peers) - 1)
	//send RequestVote
	for i := 0 ; i < len(rf.peers) ; i ++ {
		if i != me {
			go rf.startRequest(i, args, replyChan)
		}
	}
	voteCount, threshold := 0, len(rf.peers)/2
	for voteCount < threshold {
		select {
		case <- rf.shutdown:
			return
		case <- timer: //election timeout return
			fmt.Printf("server %d receive timeout\n", rf.me)
			return
		case reply := <- replyChan: // 返回投票结果
			if reply.Err != OK {
				go rf.startRequest(reply.Server, args, replyChan)
			} else if reply.VoteGranted {
				voteCount += 1
				fmt.Printf("server %d got  a vote from %d currentTime=%v \n", rf.me, reply.Server, time.Now().UnixNano() / 1e6)
			} else {
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.downToFollower(reply.Term)
					// 转为follower后，重新开启选举周期
					//rf.electionTimer.Stop()
					//rf.electionTimer.Reset(generateRandDuration(ElectiontTimeout))
					//return // 必须，否则
				}
				rf.mu.Unlock()
			}
		}
	}
	rf.mu.Lock()
	//receive enough vote, success to be leader
	if rf.state == Candidate {
		fmt.Printf("server %d become a leader %d currentTime= %v\n", rf.me, rf.currentTerm, time.Now().UnixNano() / 1e6)
		rf.state = Leader
		rf.leaderId = rf.me
		rf.initLeader()
		go rf.heartbeats()
		go func() {
			rf.applyChan <- ApplyMsg{CommandValid: false, CommandIndex: -1, CommandTerm: -1, Command: "NewLeader"}
		}()
	}
	rf.mu.Unlock()
}

func (rf *Raft) heartbeats() {
	rf.heartbeatTimer = time.NewTimer(AppendEntriesTimeout)
	exit := false
	for ; !exit ; {
		select {
		case <- rf.shutdown:
			return
		case <- rf.heartbeatTimer.C: //定时发送心跳
			_, isLeader := rf.GetState()
			if !isLeader {
				fmt.Printf("server %d change to follower, don't send heartbeat leaderId= %d currentTime= %v \n", rf.me, rf.leaderId, time.Now().UnixNano() / 1e6)
				exit = true
				return
			}
			rf.mu.Lock()
			for follower := 0 ; follower < len(rf.peers) ; follower ++ {
				if follower != rf.me {
					//发送心跳请求
					go rf.sendAppendEntries(follower)
				}
			}
			rf.heartbeatTimer.Stop()
			rf.heartbeatTimer.Reset(AppendEntriesTimeout)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) getRangeEntry(fromInclusive int, toInclusive int) []LogEntry {
	//from := rf.getOffsetIndex(fromInclusive)
	//to := rf.getOffsetIndex(toExclusive)
	startOffset := fromInclusive - rf.LastIncludedIndex
	endOffset := toInclusive - rf.LastIncludedIndex
	return append([]LogEntry{}, rf.log[startOffset:endOffset+1]...)
}

func (rf *Raft) startRequest(server int, args RequestVoteArgs, replyChan chan<- RequestVoteReply) {
	var reply RequestVoteReply
	end := rf.peers[server]
	fmt.Printf("server %d call Raft.RequestVote dst= %d, args.Term= %d, currentTime= %v \n", rf.me, server, args.Term, time.Now().UnixNano() / 1e6)
	ok := end.Call("Raft.RequestVote", &args, &reply)
	if !ok {
		reply.Err, reply.Server =  ErrRPCFail, server
		fmt.Printf("server %d call Raft.RequestVote [fail] dst= %d, args.Term= %d, currentTime= %v \n", rf.me, server, args.Term, time.Now().UnixNano() / 1e6)
	}
	replyChan <- reply
	return
}

func (rf *Raft) downToFollower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor, rf.leaderId = -1, -1
	rf.persist()
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(generateRandDuration(ElectiontTimeout))
}

func (rf *Raft) initLeader() {
	rf.nextIndex, rf.matchIndex = make([]int, len(rf.peers)), make([]int, len(rf.peers))
	lastLogIndex := rf.log[rf.getLastLogIndex()].LogIndex
	for i := 0 ; i < len(rf.peers) ; i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = lastLogIndex + 1
	}
}

func (rf *Raft) sendAppendEntries(follower int) {
	rf.mu.Lock()
	if rf.leaderId != rf.me {
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[follower] <= rf.LastIncludedIndex {
		go rf.sendSnapshot(follower)
		rf.mu.Unlock()
		return
	}
	prevLogIndex := rf.nextIndex[follower] - 1 //第一个prevLogIndex为0
	offset := prevLogIndex - rf.LastIncludedIndex
	prevLogTerm := rf.log[offset].LogTerm
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
		Len:          0,
	}
	lastLogIndex := rf.log[rf.getLastLogIndex()].LogIndex
	if rf.nextIndex[follower] <= lastLogIndex {
		entries := rf.getRangeEntry(prevLogIndex+1, lastLogIndex)
		args.Entries = entries
		args.Len = len(entries)
	}
	rf.mu.Unlock()
	var reply AppendEntriesReply
	ok := rf.peers[follower].Call("Raft.AppendEntries", &args, &reply)
	if ok {
		rf.mu.Lock()
		if reply.Success {
			prevLogIndex = args.PrevLogIndex
			logEntriesLen := args.Len
			if prevLogIndex+logEntriesLen >= rf.nextIndex[follower] { //follower成功收到后，leader更新nextIndex和matchIndex
				rf.nextIndex[follower] = prevLogIndex + logEntriesLen + 1
				rf.matchIndex[follower] = prevLogIndex + logEntriesLen
			}
			toCommitIndex := prevLogIndex + logEntriesLen
			if rf.canCommit(toCommitIndex) {
				rf.commitIndex = toCommitIndex
				rf.persist()
				rf.notifyApply <- struct{}{}
			}
		} else {
			if reply.Term > rf.currentTerm {
				rf.downToFollower(reply.Term)
			} else {
				// follower inconsistent Fixme
				lastLogIndex := rf.log[rf.getLastLogIndex()].LogIndex
				rf.nextIndex[follower] = Min(reply.ConflictIndex, lastLogIndex+1)
				if rf.nextIndex[follower] <= rf.LastIncludedIndex { // 发生conflict的时候，follower 落后太多了
					go rf.sendSnapshot(follower)
				}
			}
		}
		rf.mu.Unlock()
	}
}

// check raft can commit log entry at index
func (rf *Raft) canCommit(index int) bool {
	lastLogIndex := rf.log[rf.getLastLogIndex()].LogIndex
	if index < rf.LastIncludedIndex {
		return false
	}
	if index <= lastLogIndex && rf.commitIndex < index && rf.log[index - rf.LastIncludedIndex].LogTerm == rf.currentTerm { // 现任leader不允许提交前任leader的log
		majority, count := len(rf.peers) / 2 + 1, 0
		for i := 0 ; i < len(rf.peers) ; i ++ {
			if rf.matchIndex[i] >= index {
				count += 1
			}
		}
		return count >= majority
	} else {
		return false
	}
	return false
}

func (rf *Raft) sendSnapshot(follower int) {
	rf.mu.Lock()
	if rf.state != Leader || rf.leaderId != rf.me {
		rf.mu.Unlock()
		return
	}
	offset := rf.LastIncludedIndex - rf.LastIncludedIndex
	rf.LastIncludedTerm = rf.log[offset].LogTerm
	args := InstallSnapShotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.LastIncludedIndex,
		LastIncludedTerm:  rf.LastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
		Done: 			   true,
	}
	rf.mu.Unlock()
	var reply InstallSnapShotReply
	ok := rf.peers[follower].Call("Raft.InstallSnapshot", &args, &reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.downToFollower(reply.Term)
		} else {
			rf.nextIndex[follower] = Max(rf.nextIndex[follower], rf.LastIncludedIndex + 1)
			rf.matchIndex[follower] = Max(rf.nextIndex[follower], rf.LastIncludedIndex)
		}
		rf.mu.Unlock()
	}

}

func (rf *Raft) PersistAndSaveSnapshot(lastCommandIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("server %d start to save snapshot newLastCommandIndex= %d  log: \n", rf.me, lastCommandIndex)
	rf.printLog()
	if lastCommandIndex > rf.LastIncludedIndex {
		truncation := lastCommandIndex - rf.LastIncludedIndex
		rf.log = append([]LogEntry{}, rf.log[truncation:]...) // 空出来一个
		rf.LastIncludedIndex = lastCommandIndex
		data := rf.getPersistState()
		rf.persister.SaveStateAndSnapshot(data, snapshot)
		fmt.Printf("server %d finish to save snapshot newLastCommandIndex= %d  log: \n", rf.me, lastCommandIndex)
	}
}

func (rf *Raft) Replay(fromCommandIndex int) {
	rf.mu.Lock()
	if fromCommandIndex <= rf.LastIncludedIndex {
		rf.applyChan <- ApplyMsg{CommandValid: false, CommandIndex: rf.log[0].LogIndex, CommandTerm: rf.log[0].LogTerm, Command: "InstallSnapshot"}
		fromCommandIndex = rf.LastIncludedIndex + 1
		rf.lastApplied = Max(rf.lastApplied, rf.LastIncludedIndex)
	}
	startOffset := fromCommandIndex - rf.LastIncludedIndex
	endOffset := rf.lastApplied - rf.LastIncludedIndex
	entries := append([]LogEntry{}, rf.log[startOffset:endOffset+1]...)
	rf.mu.Unlock()
	for i := 0 ; i < len(entries) ; i ++ {
		fmt.Printf("server %d replay CommandValid= true \n", rf.me)
		rf.applyChan <- ApplyMsg{CommandValid: true, CommandIndex: entries[i].LogIndex, CommandTerm: entries[i].LogTerm, Command: entries[i].Command}
	}
	fmt.Printf("server %d replay CommandValid= false \n", rf.me)
	rf.applyChan <- ApplyMsg{CommandValid: false, CommandIndex: -1, CommandTerm: -1, Command: "ReplayDone"}
}