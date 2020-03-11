package raftkv

import (
	"fmt"
	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId 	int64
	seq			int
	leaderId	int

}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.seq = 0
	ck.leaderId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	leaderId := ck.leaderId
	args := GetArgs{Key: key}
	for {
		var reply GetReply
		fmt.Printf("client call Get, key= %s \n", key)
		if ck.servers[leaderId].Call("KVServer.Get", &args, &reply) {
			if reply.Err == OK || reply.Err == DuplicatedRequest {
				fmt.Printf("client call Get success, key= %s Err= %s \n", key, reply.Err)
				ck.leaderId = leaderId
				return reply.Value
			}
		}
		leaderId = (leaderId + 1) % len(ck.servers)
		time.Sleep(ClientTimeOut)
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seq ++
	args := PutAppendArgs{
		Key:        key,
		Value:      value,
		Op:         op,
		ClientId:   ck.clientId,
		RequestSeq: ck.seq,
	}
	leaderId := ck.leaderId
	fmt.Printf("value= %s \n", value)
	for {
		var reply PutAppendReply
		fmt.Printf("client call PutAppend, op= %s key= %s value= %s \n", op, key, value)
		if ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)  {
			if reply.Err == OK || reply.Err == DuplicatedRequest {
				fmt.Printf("client call PutAppend success, op= %s key= %s value= %s Err= %s \n", op, key, value, reply.Err)
				ck.leaderId = leaderId
				return
			}
		}
		leaderId = (leaderId + 1) % len(ck.servers)
		time.Sleep(ClientTimeOut)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
