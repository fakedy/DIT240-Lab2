package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	args := Arguments{}

	reply := Reply{}

	for {
		ok := call("Coordinator.AssignTask", &args, &reply)
		if ok {

			switch reply.TaskType {
			case MAP:
				doMAP(mapf, &reply)
				call("Coordinator.CompleteTask", &args, &reply)
			case REDUCE:
				doREDUCE(reducef, &reply)
				call("Coordinator.CompleteTask", &args, &reply)
			case WAIT:
				time.Sleep(time.Duration(1) * time.Second)
			default:
				break
			}

		} else {
			fmt.Printf("call failed!\n")
		}
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

func doMAP(mapf func(string, string) []KeyValue, reply *Reply) {

	fmt.Printf("Recieved filename:  %s\n", reply.Filename)
	// use mapf and reducef here?

	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	file.Close()

	kva := mapf(reply.Filename, string(content))

	files := make([]*os.File, reply.Nreducetasks)
	encoders := make([]*json.Encoder, reply.Nreducetasks)

	for i := 0; i < reply.Nreducetasks; i++ {
		filename := fmt.Sprintf("mr-%d-*", i)
		thatfile, err := os.CreateTemp("test", filename)
		if err != nil {

		}

		files[i] = thatfile
		enc := json.NewEncoder(thatfile)
		encoders[i] = enc
	}

	for _, kv := range kva {

		// bucket is an index
		bucket := ihash(kv.Key) % reply.Nreducetasks

		enc := encoders[bucket]
		err := enc.Encode(&kv)
		if err != nil {
			fmt.Printf("failed to encode: %v", err)
		}
	}

	for _, f := range files {
		f.Close()
	}

}

func doREDUCE(reducef func(string, []string) string, reply *Reply) {
	var kva []KeyValue

	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	file.Close()

	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}

	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in kva[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
}
