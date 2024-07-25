package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Args struct {
	// 0 means idle worker
	// 1 means complete map task
	// 2 means map task exit with error
	// 3 means complete reduce task
	// 4 means reduce task exit with error
	State int
}

type Reply struct {
	// 0 means stay idle
	// 1 means todo map Task
	// 2 means todo reduce Task
	// 3 means exit
	Task             int
	Filename         string
	NReduce          int
	ReduceTaskNumber int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
