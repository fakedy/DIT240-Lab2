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

var Nreduce int

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex
}

var ourFiles []File

type File struct {
	filename string
	state    State
}

type State int

// Enums for the worker, idle = 0, working = 1, Mapped = 2. Which iota generates.
const (
	StateIdle = iota
	Mapping
	Mapped
	Reducing
	Finished
)

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
	c.mu.Lock()
	for i := range ourFiles {
		if ourFiles[i].state == StateIdle {
			fmt.Printf("filename: %s\nstate: %d\n", ourFiles[i].filename, ourFiles[i].state)
			reply.Filename = ourFiles[i].filename
			reply.Nreducetasks = Nreduce
			reply.Id = i
			reply.TaskType = MAP
			ourFiles[i].state = Mapping // set state to "in progress"
			break
		}
	}

	// if all map tasks are done
	if mapDone() {
		for i := range ourFiles {
			if ourFiles[i].state == StateIdle {
				fmt.Printf("filename: %s\nstate: %d\n", ourFiles[i].filename, ourFiles[i].state)
				reply.Filename = ourFiles[i].filename
				reply.Id = i
				reply.TaskType = REDUCE
				ourFiles[i].state = Reducing // set state to "in progress"
				break
			}
		}
	}

	c.mu.Unlock()
	return nil
}

func mapDone() bool {

	// Your code here.
	for i := range ourFiles {
		if ourFiles[i].state != Mapped {
			return false
		}
	}

	return true

}

func (c *Coordinator) CompleteTask(args *Arguments, reply *Reply) error {

	// loop through our files and assign a task that have not been processed yet
	c.mu.Lock()
	if args.TaskType == MAP {
		ourFiles[args.Id].state = Mapped
	} else {
	}

	c.mu.Unlock()
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

	// Your code here.
	for i := range ourFiles {
		if ourFiles[i].state != Finished {
			return false
		}
	}

	return true
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
