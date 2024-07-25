package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	files            chan string
	nReduce          int
	reduceTasks      chan int
	mapUnfinished    int
	reduceUnfinished int
	mu               sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RPCHandler(args *Args, reply *Reply) error {
	// fmt.Printf("RPC handler called, args.State: %d \n", args.State)
	if c.Done() {
		reply.Task = 3
		return nil
	}
	switch args.State {
	case 0:
		// distribute task
		// fmt.Printf("distribute task, file len is %d, reduceTasks is %d \n", len(c.files), len(c.reduceTasks))
		if c.mapUnfinished != 0 {
			if len(c.files) == 0 {
				// stay idle
				reply.Task = 0
				return nil
			}
			reply.Filename = <-c.files
			reply.NReduce = c.nReduce
			reply.Task = 1
		} else {
			if len(c.reduceTasks) == 0 {
				// stay idle
				reply.Task = 0
				return nil
			}
			reply.ReduceTaskNumber = <-c.reduceTasks
			reply.Task = 2
		}
	case 1:
		// count map task done
		// fmt.Printf("map task finished: %s \n", reply.Filename)
		c.mu.Lock()
		c.mapUnfinished--
		c.mu.Unlock()
	case 2:
		// restore map task
		// fmt.Printf("map task crashes: %s \n", reply.Filename)
		c.files <- reply.Filename
	case 3:
		// count reduce task done
		// fmt.Printf("reduce task finished: %d \n", reply.ReduceTaskNumber)
		c.mu.Lock()
		c.reduceUnfinished--
		c.mu.Unlock()
	case 4:
		// restore reduce task
		// fmt.Printf("reduce task crashes: %d \n", reply.ReduceTaskNumber)
		c.reduceTasks <- reply.ReduceTaskNumber
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.mapUnfinished == 0 && c.reduceUnfinished == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.nReduce = nReduce
	c.mapUnfinished = len(files)
	c.reduceUnfinished = nReduce

	c.files = make(chan string, len(files))
	for _, filename := range files {
		c.files <- filename
	}
	// fmt.Printf("file len is %d \n", len(c.files))

	c.reduceTasks = make(chan int, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks <- i
	}
	// fmt.Printf("reduce task len is %d \n", len(c.reduceTasks))

	c.server()
	return &c
}
