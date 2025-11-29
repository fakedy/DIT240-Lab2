package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	for {
		args := Arguments{}
		reply := Reply{}
		ok := call("Coordinator.AssignTask", &args, &reply)
		completeReply := Reply{}
		completeArgs := Arguments{reply.Id, reply.TaskType}
		if ok {

			switch reply.TaskType {
			case MAP:
				doMAP(mapf, &reply)
				call("Coordinator.CompleteTask", &completeArgs, &completeReply)
			case REDUCE:
				doREDUCE(reducef, &reply)
				call("Coordinator.CompleteTask", &completeArgs, &completeReply)
			case WAIT:
				time.Sleep(time.Duration(1) * time.Second)
			default:
				break
			}

		} else {
			//fmt.Printf("Call failed!\n")
			return
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
		//log.Fatal("dialing:", err)
		return false
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

	//fmt.Printf("Performing MAP on file:  %s\n", reply.Filename)
	// use mapf and reducef here?

	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("Could not open: %v", reply.Filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Could not read: %v", reply.Filename)
	}
	file.Close()

	kva := mapf(reply.Filename, string(content))

	files := make([]*os.File, reply.Nreducetasks)
	encoders := make([]*json.Encoder, reply.Nreducetasks)

	// create NReduce intermediate files
	for i := 0; i < reply.Nreducetasks; i++ {
		filename := fmt.Sprintf("mr-%d-*", reply.Id)
		thatfile, err := os.CreateTemp(".", filename)
		if err != nil {
			log.Fatalf("Could not create file: %v", filename)
		}

		// add these files and their encoder to an array
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
			fmt.Printf("Failed to encode: %v\n", err)
		}
	}

	for i, f := range files {
		f.Close()
		finalName := fmt.Sprintf("mr-%d-%d", reply.Id, i)
		os.Rename(f.Name(), finalName)
		if err != nil {
			log.Fatalf("Could not rename file\n")
		}
	}

}

func doREDUCE(reducef func(string, []string) string, reply *Reply) {

	//fmt.Printf("Performing reduce on task ID: %d\n", reply.Id)
	var kva []KeyValue

	var files []os.DirEntry

	//read all files in current directory
	allfiles, err := os.ReadDir(".")
	if err != nil {
		log.Fatal("read error", err)
	}

	//prefix for files with name mr-ID
	fileSuffix := fmt.Sprintf("-%d", reply.Id)

	//filter out the files with with name mr-ID from all files
	for _, file := range allfiles {
		fileName := file.Name()
		if strings.HasPrefix(fileName, "mr-") && strings.HasSuffix(fileName, fileSuffix) && !strings.Contains(fileName, "mr-out") {
			files = append(files, file)
		}
	}

	//open each file and decodes json thing and then rebuilds kva from contents
	for _, f := range files {
		file, err := os.Open(f.Name())
		if err != nil {
			log.Fatal("Could not open file\n", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	//sort kva
	sort.Sort(ByKey(kva))

	//create file with name mr-out-ID for reduce output
	oname := fmt.Sprintf("mr-out-%d", reply.Id)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in kva[],
	// and print the result to mr-out-ID.
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

	// remove intermediate files
	//for _, f := range files {
	//	os.Remove(f.Name())
	//}
}
