package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// fmt.Println("worker called")
	for {
		// ask for a task
		// fmt.Println("ask for a task")
		args := Args{}
		reply := Reply{}
		ok := call("Coordinator.RPCHandler", &args, &reply)
		if !ok {
			fmt.Printf("call failed!\n")
		}

		switch reply.Task {
		case 0:
			// stay idle
			// fmt.Println("stay idle")
			time.Sleep(100 * time.Millisecond)
		case 1:
			// start map task
			// fmt.Printf("start map task, filename: %s \n", reply.Filename)
			filename := reply.Filename
			file, err := os.Open(filename)
			if err != nil {
				args.State = 2
				call("Coordinator.RPCHandler", &args, &reply)
				fmt.Printf("cannot open %v", filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				args.State = 2
				call("Coordinator.RPCHandler", &args, &reply)
				fmt.Printf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))

			// create the list of temp files
			// fmt.Println("create the list of temp files")
			nReduce := reply.NReduce
			intermediateTempFiles := make([]*os.File, nReduce)
			encoders := make([]*json.Encoder, nReduce)
			mapTaskNumber := ihash(filename) % nReduce
			for i := 0; i < nReduce; i++ {
				filename := fmt.Sprintf("temp-%d-%d", mapTaskNumber, i)
				file, err := os.CreateTemp("", filename)
				if err != nil {
					args.State = 2
					call("Coordinator.RPCHandler", &args, &reply)
					fmt.Printf("cannot create file %s", filename)
				}
				intermediateTempFiles[i] = file
				encoders[i] = json.NewEncoder(file)
			}

			// partition and write key-value pairs to files
			// fmt.Println("partition and write key-value pairs to files")
			for _, kv := range kva {
				partition := ihash(kv.Key) % nReduce
				err := encoders[partition].Encode(&kv)
				if err != nil {
					args.State = 2
					call("Coordinator.RPCHandler", &args, &reply)
					fmt.Printf("cannot write to file %s", intermediateTempFiles[partition].Name())
				}
			}

			// rename temp files
			// fmt.Println("rename temp files")
			for i := 0; i < nReduce; i++ {
				tempFileName := intermediateTempFiles[i].Name()
				finalFileName := fmt.Sprintf("mr-%d-%d", mapTaskNumber, i)
				err := intermediateTempFiles[i].Close()
				if err != nil {
					args.State = 2
					call("Coordinator.RPCHandler", &args, &reply)
					fmt.Printf("cannot close temp file %s", tempFileName)
				}
				if _, err := os.Stat(finalFileName); err == nil {
					// final file exists, append context in the temp file to it
					dest, err := os.OpenFile(finalFileName, os.O_APPEND|os.O_WRONLY, 0666)
					if err != nil {
						args.State = 2
						call("Coordinator.RPCHandler", &args, &reply)
						fmt.Printf("cannot open dest file %v for appending", finalFileName)
					}
					source, err := os.Open(tempFileName)
					if err != nil {
						args.State = 2
						call("Coordinator.RPCHandler", &args, &reply)
						fmt.Printf("Cannot open source file %v: %v", tempFileName, err)
					}
					_, err = io.Copy(dest, source)
					if err != nil {
						args.State = 2
						call("Coordinator.RPCHandler", &args, &reply)
						fmt.Printf("cannot append content from %v to %v", tempFileName, finalFileName)
					}
					dest.Close()
					source.Close()
				} else {
					err = os.Rename(tempFileName, finalFileName)
					if err != nil {
						args.State = 2
						call("Coordinator.RPCHandler", &args, &reply)
						fmt.Printf("cannot rename temp file %s to final file %s", tempFileName, finalFileName)
					}
				}
			}

			args.State = 1
			// fmt.Println(reply.Filename)
			call("Coordinator.RPCHandler", &args, &reply)
		case 2:
			// start reduce task
			// fmt.Printf("start reduce task: %d \n", reply.ReduceTaskNumber)
			// find all the intermediate file with the reduce task number
			filenamePattern := fmt.Sprintf("mr-*-%d", reply.ReduceTaskNumber)
			files, err := filepath.Glob(filenamePattern)
			if err != nil {
				args.State = 4
				call("Coordinator.RPCHandler", &args, &reply)
				fmt.Printf("error finding files: %v", err)
			}
			if len(files) == 0 {
				return
			}
			// sort all the KV structs into a map
			// fmt.Println("sort all the KV structs into a map")
			intermediate := []KeyValue{}
			for _, filename := range files {
				// fmt.Println(filename)
				if fileInfo, _ := os.Stat(filename); fileInfo.Size() == 0 {
					// fmt.Printf("file %s is empty", filename)
					continue
				}
				file, err := os.Open(filename)
				if err != nil {
					args.State = 4
					call("Coordinator.RPCHandler", &args, &reply)
					fmt.Printf("cannot open file %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						if err.Error() == "EOF" {
							break
						}
						args.State = 4
						call("Coordinator.RPCHandler", &args, &reply)
						fmt.Print("cannot decode key-value from file", filename)
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
			sort.Sort(ByKey(intermediate))
			// do reduce task for all the key value pairs in the map
			// store all the result into an output file
			// fmt.Println("do reduce task for all the key value pairs in the map")
			outputFileName := fmt.Sprintf("mr-out-%d", reply.ReduceTaskNumber)
			outputFile, err := os.Create(outputFileName)
			if err != nil {
				args.State = 4
				call("Coordinator.RPCHandler", &args, &reply)
				fmt.Printf("cannot create output file %v", outputFileName)
			}
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			outputFile.Close()

			args.State = 3
			call("Coordinator.RPCHandler", &args, &reply)
		case 3:
			// exit
			// fmt.Println("exit")
			return
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
