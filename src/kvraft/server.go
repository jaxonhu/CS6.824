package raftkv

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

}

type notifyRes struct {
	Term int
	Value string
	Error Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister 	*raft.Persister
	kvs			map[string]string
	table 		map[int64]int
	shutdown 	chan struct{}
	notifyChanMap map[int]chan notifyRes
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Err, reply.Value = kv.start(args.copy())
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err, _ = kv.start(args.copy())
	fmt.Printf("kvserver PutAppend start over Err= %s \n", reply.Err)
}

func (kv *KVServer) start(args interface{}) (Err, string) {
	index, term, ok := kv.rf.Start(args)
	fmt.Printf("kvserver got reply ok= %v \n", ok)
	if !ok {
		return ErrWrongLeader, ""
	}
	kv.mu.Lock()
	notifyChan := make(chan notifyRes)
	kv.notifyChanMap[index] = notifyChan
	kv.mu.Unlock()
	select {
		case <- time.After(RequestTimeOut):
			kv.mu.Lock()
			//fmt.Printf("kvserver %d request timeout \n", kv.me)
			delete(kv.notifyChanMap, index)
			kv.mu.Unlock()
			return ServerTimeOut, ""
		case res := <- notifyChan:
			if res.Term != term {
				//fmt.Printf("kvserver %d term not match \n", kv.me)
				return RaftTermError, ""
			}
			//fmt.Printf("kvserver %d success reply a value to client \n", kv.me)
			return res.Error, res.Value
	}
	return OK, ""
}

func (kv *KVServer) apply(msg raft.ApplyMsg) {
	result := notifyRes{
		Term:  msg.CommandTerm,
		Value: "",
		Error: OK,
	}
	if args, ok := msg.Command.(GetArgs); ok {
		result.Value = kv.kvs[args.Key]
	} else if args, ok := msg.Command.(PutAppendArgs); ok {
		if kv.table[args.ClientId] < args.RequestSeq {
			if args.Op == "Put" {
				kv.kvs[args.Key] = args.Value
			} else {
				kv.kvs[args.Key] += args.Value
			}
			kv.table[args.ClientId] = args.RequestSeq
		} else {
			result.Error = DuplicatedRequest
		}
	}
	// 返回客户端请求结果
	//fmt.Printf("kvserver %d got a applymsg msgCommandIndex= %d \n", kv.me, msg.CommandIndex)
	if ch, ok := kv.notifyChanMap[msg.CommandIndex]; ok {
		//fmt.Printf("kvserver %d reply to client \n", kv.me)
		delete(kv.notifyChanMap, msg.CommandIndex)
		ch <- result
	}
	// 是否需要snapshot
	threshold := kv.maxraftstate
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= threshold {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.table)
		e.Encode(kv.kvs)
		snapshot := w.Bytes()
		kv.rf.PersistAndSaveSnapshot(msg.CommandIndex, snapshot)
	}

}

func (kv *KVServer) readSnapshot() {
	snapshot := kv.persister.ReadSnapshot()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.table) != nil || d.Decode(&kv.kvs) != nil {
		fmt.Printf("kvserver %d readSnapshot error", kv.me)
	}

}

func (kv *KVServer) run() {
	// replay
	go kv.rf.Replay(1)
	for {
		select {
			case msg := <-kv.applyCh:
				kv.mu.Lock()
				fmt.Printf("kvserver %d receive a msg msgValid= %v \n", kv.me, msg.CommandValid)
				fmt.Println(msg.Command)
				if msg.CommandValid {
					kv.apply(msg)
				} else if cmd, ok := msg.Command.(string); ok {
					fmt.Printf("msg command= %s \n", cmd)
					if cmd == "InstallSnapshot" {
						kv.readSnapshot()
					}
				}
				kv.mu.Unlock()
			case <- kv.shutdown:
				return
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	fmt.Printf("kvserver %d kill \n", kv.me)
	// Your code here, if desired.
	close(kv.shutdown)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg, 1000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvs = make(map[string]string)
	kv.table = make(map[int64]int)
	kv.shutdown = make(chan struct{})
	kv.notifyChanMap = make(map[int]chan notifyRes)
	// You may need initialization code here.
	go kv.run()
	return kv
}
