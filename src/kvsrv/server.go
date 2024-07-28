package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	KVmap map[string]string
	IDs   map[int64]bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.KVmap[args.Key]
	if !ok {
		reply.Value = ""
	} else {
		reply.Value = value
	}
	DPrintf("Server performed get")
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.IDs[args.ID] {
		reply.Value = kv.KVmap[args.Key]
		// delete(kv.IDs, args.ID)
		return
	}
	reply.Value = kv.KVmap[args.Key]
	kv.KVmap[args.Key] = args.Value
	kv.IDs[args.ID] = true
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.IDs[args.ID] {
		reply.Value = kv.KVmap[args.Key]
		// delete(kv.IDs, args.ID)
		return
	}
	reply.Value = kv.KVmap[args.Key]
	kv.KVmap[args.Key] = kv.KVmap[args.Key] + args.Value
	kv.IDs[args.ID] = true
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.KVmap = make(map[string]string)
	kv.IDs = make(map[int64]bool)

	return kv
}
