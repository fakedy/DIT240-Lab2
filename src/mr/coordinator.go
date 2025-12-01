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
	mapTasks          []Task
	reduceTasks       []Task
}

type Task struct {
	filename string
	state    State
}

type State int

// Enums for the worker, idle = 0, working = 1, Finished = 2. Which iota generates.
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
		for i := range c.mapTasks {
			if c.mapTasks[i].state == StateIdle {
				//fmt.Printf("filename: %s state: %d ID: %d\n", mapTasks[i].filename, mapTasks[i].state, i)
				reply.Filename = c.mapTasks[i].filename
				reply.Nreducetasks = c.Nreduce
				reply.Id = i
				reply.TaskType = MAP
				c.mapTasks[i].state = Working // set state to "in progress"
				c.mapWorkerTimes[i] = time.Now()
				c.mu.Unlock()
				//fmt.Print("Running map job\n")
				return nil
			}
		}
	} else {
		// if all map tasks are done
		// if the reduce task is idle, then make it a working reduce task
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

	// If we can't assign either, then assign WAIT
	reply.TaskType = WAIT

	c.mu.Unlock()
	return nil
}

func (c *Coordinator) mapDone() bool {

	// for every map task, if the task is less than Finished (since idle = 0, working = 1, Finished = 2.)
	// then return false since all map tasks are not done.
	for i := range c.mapTasks {
		if c.mapTasks[i].state < Finished {
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
		// check if all tasks are mapped
		c.mapTasks[args.Id].state = Finished
		c.isMappingDone = c.mapDone()
	case REDUCE:
		c.reduceTasks[args.Id].state = Finished
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
	// if all reduce tasks are done, return true otherwise false
	for i := range c.reduceTasks {
		if c.reduceTasks[i].state != Finished {
			c.mu.Unlock()
			return false
		}
	}

	c.mu.Unlock()
	return true
}

// Check if a workers time since start has reached over 10 second (e.g crashed, unresponsive) then change to idle state
func (c *Coordinator) checkWorkers() {

	for {
		time.Sleep(time.Second)
		c.mu.Lock()
		for i := range c.mapWorkerTimes {
			if c.mapTasks[i].state == Working && time.Since(c.mapWorkerTimes[i]) >= 10*time.Second {
				c.mapTasks[i].state = StateIdle
			}

		}
		for i := range c.reduceTasks {
			if c.reduceTasks[i].state == Working && time.Since(c.reduceWorkerTimes[i]) >= 10*time.Second {
				c.reduceTasks[i].state = StateIdle
			}
		}
		c.mu.Unlock()
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
		c.mapTasks = append(c.mapTasks, Task{filename: files[i], state: StateIdle})
	}

	c.Nreduce = nReduce

	// Allocate spaces for worker times for every map tasks and reduce tasks
	c.mapWorkerTimes = append(c.mapWorkerTimes, make([]time.Time, len(files))...)

	c.reduceWorkerTimes = append(c.reduceWorkerTimes, make([]time.Time, nReduce)...)

	c.reduceTasks = append(c.reduceTasks, make([]Task, nReduce)...)

	// Start a go routine that checks if a worker has crashed etc.
	go c.checkWorkers()

	c.server()
	return &c
}
