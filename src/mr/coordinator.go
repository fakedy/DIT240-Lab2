package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

// Our current state of the worker
type State int

var Nreduce int

// Enums for the worker, idle = 0, working = 1. Which iota generates.
const (
	StateIdle = iota
	StateWorking
)

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
}

var ourFiles []File

type File struct {
	filename string
	state    State
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *Arguments, reply *Reply) error {

	// loop through our files and assign a task that have not been processed yet
	for i := range ourFiles {
		fmt.Printf("filename: %s\nstate: %d\n", ourFiles[i].filename, ourFiles[i].state)
		c.mu.Lock()
		if ourFiles[i].state == StateIdle {
			reply.Filename = ourFiles[i].filename
			reply.Nreducetasks = Nreduce
			ourFiles[i].state = StateWorking // set state to "in progress"
			return nil
		}
		c.mu.Unlock()
	}
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	// fill an array of File structs
	for i := range files {
		ourFiles = append(ourFiles, File{filename: files[i], state: StateIdle})
	}

	for i := range ourFiles {
		fmt.Println(ourFiles[i].filename)
	}

	Nreduce = nReduce

	c.server()
	return &c
}
