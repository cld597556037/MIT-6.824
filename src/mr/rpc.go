package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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
type TaskType int

const TaskMap = 0
const TaskReduce = 1
const TaskEmpty = 2
const TaskDone = 3

// GetTaskArgs
type GetTaskArgs struct {
}

type GetTaskReply struct {
	TaskType TaskType
	TaskNo   int
	FileName string
	NReduce  int //only for map task
	NMap     int
}

type FinishTaskArgs struct {
	TaskType TaskType
	TaskNo   int
}

type FinishTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
