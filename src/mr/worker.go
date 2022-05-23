package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		task := CallGetTask()
		if task == nil {
			log.Fatal("CallGetTask failed!\n")
			return
		}
		log.Printf("Task Info:%v!\n", task)
		if task.TaskType == TaskEmpty {
			time.Sleep(time.Second)
		} else if task.TaskType == TaskDone {
			break
		} else if task.TaskType == TaskMap {
			filename := task.FileName
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
				return
			}
			file.Close()
			kvs := mapf(filename, string(content))

			//partiton into nReduce files
			intermediateFiles := make([]*os.File, task.NReduce)
			encs := make([]*json.Encoder, task.NReduce)
			for i := 0; i < len(intermediateFiles); i++ {
				file, _ = os.Create(fmt.Sprintf("mr-%v-%v", task.TaskNo, i))
				intermediateFiles[i] = file
				encs[i] = json.NewEncoder(file)
			}

			for _, kv := range kvs {
				hashId := ihash(kv.Key)
				enc := encs[hashId%task.NReduce]
				if err := enc.Encode(&kv); err != nil {
					log.Fatalf("json encoder encode err, %s", err.Error())
					return
				}
			}

			for i := 0; i < len(intermediateFiles); i++ {
				intermediateFiles[i].Close()
			}
			CallFinishTask(task.TaskNo, TaskMap)

		} else if task.TaskType == TaskReduce {

			kvsm := make(map[string][]string)
			for i := 0; i < task.NMap; i++ {
				filename := fmt.Sprintf("mr-%v-%v", i, task.TaskNo)
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					var vals []string
					if _, ok := kvsm[kv.Key]; !ok {
						vals = make([]string, 0)
					} else {
						vals = kvsm[kv.Key]
					}
					kvsm[kv.Key] = append(vals, kv.Value)
				}
				file.Close()
			}

			oname := fmt.Sprintf("mr-out-%v", task.TaskNo)
			ofile, _ := os.Create(oname)
			for k, vals := range kvsm {
				value := reducef(k, vals)
				fmt.Fprintf(ofile, "%v %v\n", k, value)
			}
			ofile.Close()
			CallFinishTask(task.TaskNo, TaskReduce)
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

func CallGetTask() *GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		return &reply
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}
}

func CallFinishTask(taskNo int, taskType TaskType) *FinishTaskReply {
	args := FinishTaskArgs{
		TaskType: taskType,
		TaskNo:   taskNo,
	}
	reply := FinishTaskReply{}
	ok := call("Coordinator.FinishTask", &args, &reply)
	if ok {
		return &reply
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
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
