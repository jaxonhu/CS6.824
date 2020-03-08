package raft

import (
	"math/rand"
	"time"
)

func Min(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

type serverState int32
const (
	Leader serverState = iota
	Follower
	Candidate
)

const (
	OK         = "OK"
	ErrRPCFail = "ErrRPCFail"
)

type LogEntry struct {
	LogIndex int
	LogTerm int
	Command interface{}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
	Err string
	Server int
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
	Len int
}

type AppendEntriesReply struct {
	Term int
	Success bool
	ConflictIndex int // in case of conflicting, follower include the first index it store for conflict term
}

type InstallSnapShotArgs struct {
	Term int  					// leader's term
	LeaderId int 				// follower redirect clients
	LastIncludedIndex int		// the snapshot replaces all entries up through and including this index
	LastIncludedTerm int		//
	Offset int					// byte offset where chunk is positioned in the snapshot file
	Data []byte					// raw bytes of the snapshot chunk, starting at offset
	Done bool					// true if this is the last chunk
}

type InstallSnapShotReply struct {
	Term int // currentTerm, for leader to update itself
}

const ElectiontTimeout = time.Duration(1000 * time.Millisecond)
const AppendEntriesTimeout = time.Duration(100 * time.Millisecond)

func generateRandDuration(minDuration time.Duration) time.Duration {
	extra := time.Duration(rand.Int63()) % minDuration
	return time.Duration(minDuration + extra)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}