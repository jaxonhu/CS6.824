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
	"fmt"
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
	CommandValid bool
	Command      interface{}
	CommandIndex int
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
	leaderId int				  // leader's id
	currentTerm int				  // latest term server has seen, initialized to 0
	votedFor int				  // candidate that received vote in current term
	logIndex int				  // index of next log entry to be stored, initialized to 1
	commitIndex int 			  // index of highest log entry known to be committed, initialized to 0
	lastApplied int				  // index of highest log entry applied to state machine, initialized to 0
	state serverState 			  // state of server
	log []LogEntry 				  // log entries
	nextIndex []int				  // for each server, index of the next log entry to send to that server
	matchIndex []int              // for each server, index of highest log entry, used to track committed index
	applyChan chan ApplyMsg 	  // apply to client

	shutdown chan struct{}
	electionTimer *time.Timer

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	// Your code here (2A).
	term = rf.currentTerm
	return term, rf.state == Leader
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
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	index := rf.logIndex
	term := rf.currentTerm
	if rf.state != Leader {
		return -1, -1, false
	}
	entry := LogEntry{
		LogIndex: index,
		LogTerm:  term,
		Command:  command,
	}
	rf.log = append(rf.log, entry)
	rf.matchIndex[rf.me] = rf.logIndex  //
	rf.logIndex += 1
	go func() {
		for follower := 0 ; follower <= len(rf.peers) ; follower ++ {
			if follower != rf.me {
				go rf.sendAppendEntries(follower)
			}
		}
	}()
	return index, term, true
}


//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf.logIndex = 1
	rf.log =  []LogEntry{{0, 0, nil}} // log entry at index 0 is unused
	rf.electionTimer = time.NewTimer(generateRandDuration(ElectiontTimeout))
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// 作为follower监听AppendEntries请求
	go rf.accpet()
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

func (rf *Raft) accpet() {
	// Todo

}

func (rf *Raft) campaign() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		return
	}
	rf.state = Candidate
	rf.leaderId = -1
	rf.currentTerm += 1
	rf.votedFor = rf.me
	lastLogIndex := rf.logIndex - 1
	lastLogTerm := rf.log[lastLogIndex].LogTerm
	fmt.Printf("server %d begin to vote for leader\n", rf.me)
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

	replyChan := make(chan RequestVoteReply, len(rf.peers) - 1)
	//send RequestVote
	for i := 0 ; i < len(rf.peers) ; i ++ {
		if i != rf.me {
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
				fmt.Printf("server %d got  a vote from %d \n", rf.me, reply.Server)
			} else {
				if reply.Term > rf.currentTerm {
					rf.downToFollower(reply.Term)
					// 转为follower后，重新开启选举周期
					rf.electionTimer.Stop()
					rf.electionTimer.Reset(generateRandDuration(ElectiontTimeout))
					return
				}
			}
		}
	}

	//receive enough vote, success to be leader
	if rf.state == Candidate {
		fmt.Printf("server %d become a leader %d \n", rf.me, rf.currentTerm)
		rf.state = Leader
		rf.leaderId = rf.me
		rf.initLeader()
		go rf.heartbeats()
	}
}

func (rf *Raft) heartbeats() {
	timer := time.NewTimer(AppendEntriesTimeout)
	for {
		select {
		case <- rf.shutdown:
			return
		case <- timer.C: //定时发送心跳
			if _, isLeader := rf.GetState(); !isLeader {
				return
			}
			go func() {
				for follower := 0 ; follower < len(rf.peers) ; follower ++ {
					if follower != rf.me {
						//发送心跳请求
						//Todo
						go rf.sendAppendEntries(follower)
					}
				}
			}()
			timer.Reset(AppendEntriesTimeout)
		}
	}
}

func (rf *Raft) getRangeEntry(fromInclusive, toExclusive int) []LogEntry {
	//from := rf.getOffsetIndex(fromInclusive)
	//to := rf.getOffsetIndex(toExclusive)
	return append([]LogEntry{}, rf.log[fromInclusive:toExclusive]...)
}

func (rf *Raft) startRequest(server int, args RequestVoteArgs, replyChan chan<- RequestVoteReply) {
	var reply RequestVoteReply
	end := rf.peers[server]
	ok := end.Call("Raft.RequestVote", &args, &reply)
	if !ok {
		reply.Err, reply.Server =  ErrRPCFail, server
	}
	replyChan <- reply
	return
}

func (rf *Raft) downToFollower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor, rf.leaderId = -1, -1
	//rf.persist()
}

func (rf *Raft) initLeader() {
	rf.nextIndex, rf.matchIndex = make([]int, len(rf.peers)), make([]int, len(rf.peers))
	for i := 0 ; i < len(rf.peers) ; i++ {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = rf.commitIndex + 1
	}
}

func (rf *Raft) sendAppendEntries(follower int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.leaderId != rf.me {
		return
	}
	prevLogIndex := rf.nextIndex[follower] - 1 //第一个prevLogIndex为0
	prevLogTerm := rf.log[prevLogIndex].LogTerm
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
		Len:          0,
	}
	if rf.nextIndex[follower] < rf.logIndex {
		entries := rf.getRangeEntry(prevLogIndex+1, rf.logIndex)
		args.Entries = entries
		args.Len = len(entries)
	}

	var reply AppendEntriesReply
	fmt.Printf("server %d call Raft.AppendEntries, Term= %d, LeaderId= %d, PrevLogIndex= %d, " +
		"PrevLogTerm= %d, LeaderCommit= %d, Len= %d \n", rf.me, rf.currentTerm, rf.me, prevLogIndex,
		prevLogTerm, rf.commitIndex, args.Len)
	ok := rf.peers[follower].Call("Raft.AppendEntries", &args, &reply)
	if ok {
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
				//rf.notifyApplyCh <- struct{}{}  Fixme
			}
		} else {
			if reply.Term > rf.currentTerm {
				rf.downToFollower(reply.Term)
			} else {
				// follower inconsistent Fixme
				rf.nextIndex[follower] = reply.ConflictIndex
			}
		}
	}
}

// check raft can commit log entry at index
func (rf *Raft) canCommit(index int) bool {
	if index < rf.logIndex && rf.commitIndex < index && rf.log[index].LogTerm == rf.currentTerm {
		majority, count := len(rf.peers) / 2 + 1, 0
		for i := 0 ; i < len(rf.peers) ; i ++ {
			if rf.matchIndex[i] >= index {
				count += 1
			}
			return count >= majority
		}
	} else {
		return false
	}
	return false
}
