package kvsrv

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	DPrintf("Get called, get key: %s \n", key)
	args := GetArgs{
		Key: key,
	}
	reply := GetReply{}
	for {
		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			break
		}
		time.Sleep(100 * time.Microsecond)
	}
	DPrintf("Returned value: %s \n", reply.Value)
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	DPrintf("PutAppend called, key: %s, value: %s \n", key, value)
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		ID:    nrand(),
	}
	reply := PutAppendReply{}
	for {
		ok := ck.server.Call("KVServer."+op, &args, &reply)
		if ok {
			break
		}
		time.Sleep(100 * time.Microsecond)
	}
	DPrintf("PutAppend Returned value: %s \n", reply.Value)
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("Put called, key: %s, value: %s \n", key, value)
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	DPrintf("Append called, key: %s, value: %s \n", key, value)
	return ck.PutAppend(key, value, "Append")
}
