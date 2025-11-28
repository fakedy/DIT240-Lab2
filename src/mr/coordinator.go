package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu                sync.Mutex
	isMappingDone     bool
	mapWorkerTimes    []time.Time
	reduceWorkerTimes []time.Time
	Nreduce           int
	ourFiles          []Task
	reduceTasks       []Task
}

type Task struct {
	filename string
	state    State
}

type State int

// Enums for the worker, idle = 0, working = 1, Mapped = 2. Which iota generates.
const (
	StateIdle = iota
	Working
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

	// if mapping is not done
	if !c.isMappingDone {
		for i := range c.ourFiles {
			if c.ourFiles[i].state == StateIdle {
				//fmt.Printf("filename: %s state: %d ID: %d\n", ourFiles[i].filename, ourFiles[i].state, i)
				reply.Filename = c.ourFiles[i].filename
				reply.Nreducetasks = c.Nreduce
				reply.Id = i
				reply.TaskType = MAP
				c.ourFiles[i].state = Working // set state to "in progress"
				c.mapWorkerTimes[i] = time.Now()
				c.mu.Unlock()
				return nil
			}
		}
	} else {
		// if all map tasks are done
		for i := range c.reduceTasks {
			if c.reduceTasks[i].state == StateIdle {
				reply.Id = i
				reply.TaskType = REDUCE
				c.reduceTasks[i].state = Working // set state to "in progress"
				c.reduceWorkerTimes[i] = time.Now()
				//fmt.Printf("Starting Reduce task on ID: %d\n", reply.Id)
				c.mu.Unlock()
				return nil
			}
		}
	}

	reply.TaskType = WAIT

	c.mu.Unlock()
	return nil
}

func (c *Coordinator) mapDone() bool {

	// Your code here.
	for i := range c.ourFiles {
		if c.ourFiles[i].state < Finished {
			//fmt.Println("Mapping not done")
			return false
		}
	}

	//fmt.Println("Mapping done")
	return true
}

func (c *Coordinator) CompleteTask(args *Arguments, reply *Reply) error {

	// loop through our files and assign a task that have not been processed yet
	c.mu.Lock()
	switch args.TaskType {
	case MAP:
		c.ourFiles[args.Id].state = Finished
		//fmt.Printf("Map Task completed: %d\n", args.Id)
		// check if all tasks are mapped
		c.isMappingDone = c.mapDone()
	case REDUCE:
		c.reduceTasks[args.Id].state = Finished
		//fmt.Printf("Reduce Task completed with ID: %d\n", args.Id)
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
	c.mu.Lock()
	// Your code here.
	for i := range c.reduceTasks {
		if c.reduceTasks[i].state != Finished {
			c.mu.Unlock()
			return false
		}
	}

	c.mu.Unlock()
	return true
}

func (c *Coordinator) checkWorkers() {

	for {
		c.mu.Lock()
		for i := range c.mapWorkerTimes {
			if c.ourFiles[i].state == Working && time.Since(c.mapWorkerTimes[i]) >= time.Duration(10)*time.Second {
				c.ourFiles[i].state = StateIdle
			} else if c.reduceTasks[i].state == Working && time.Since(c.reduceWorkerTimes[i]) >= time.Duration(10)*time.Second {
				c.reduceTasks[i].state = StateIdle
			}

		}
		c.mu.Unlock()
		time.Sleep(time.Second * time.Duration(10))
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.isMappingDone = false

	// fill an array of File structs
	for i := range files {
		c.ourFiles = append(c.ourFiles, Task{filename: files[i], state: StateIdle})
	}

	c.Nreduce = nReduce

	// Append 5 nil pointers to the end
	c.mapWorkerTimes = append(c.mapWorkerTimes, make([]time.Time, len(files))...)

	c.reduceWorkerTimes = append(c.reduceWorkerTimes, make([]time.Time, nReduce)...)

	c.reduceTasks = append(c.reduceTasks, make([]Task, nReduce)...)

	go c.checkWorkers()

	c.server()
	return &c
}
