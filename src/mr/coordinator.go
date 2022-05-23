package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

type Status int

const StatusInitiated = 0
const StatusProcessing = 1
const StatusSucceeded = 2
const StatusFailed = 3
const StatusTimeout = 4

type Coordinator struct {
	// Your definitions here.
	files          []string
	nReduce        int
	MapStatuses    []Status
	ReduceStatuses []Status
	mutex          *sync.Mutex
	tasks          []*taskInfo
}

type taskInfo struct {
	t           TaskType
	status      Status
	expiredTime time.Time
	filename    string
	no          int
}

func (t *taskInfo) isAssignable() bool {
	return t.status == StatusInitiated || t.status == StatusFailed || t.status == StatusTimeout
}

func (t *taskInfo) isFinished() bool {
	return t.status == StatusSucceeded
}

func (t *taskInfo) isExpired() bool {
	return t.status == StatusProcessing && t.expiredTime.Before(time.Now())
}

func (t *taskInfo) timeout() {
	log.Printf("%v, %v timeout", t.t, t.no)
	t.status = StatusTimeout
}

func (t *taskInfo) finish() {
	log.Printf("%v, %v finish", t.t, t.no)
	t.status = StatusSucceeded
}

func (t *taskInfo) start() {
	log.Printf("%v, %v start", t.t, t.no)
	t.status = StatusProcessing
	t.expiredTime = time.Now().Add(time.Second * 10)
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mutex.Lock()
	defer func() {
		log.Printf("GetTaskReply: %v ", reply)
	}()
	defer c.mutex.Unlock()
	log.Printf("GetTaskArgs: %v ", args)
	//优先分配map任务
	mapTask := c.getAvailableTask(TaskMap)
	if mapTask != nil {
		mapTask.start()
		reply.TaskNo = mapTask.no
		reply.FileName = mapTask.filename
		reply.TaskType = TaskMap
		reply.NReduce = c.nReduce
		return nil
	}

	if !c.isTaskFinished(TaskMap) {
		//map任务执行完，才能分配reduce任务
		log.Println("all map tasks assigned but not finished")
		reply.TaskType = TaskEmpty
		return nil
	}

	reduceTask := c.getAvailableTask(TaskReduce)
	if reduceTask != nil {
		reduceTask.start()
		reply.TaskNo = reduceTask.no
		reply.TaskType = TaskReduce
		reply.NMap = len(c.files)
		return nil
	}

	if !c.isTaskFinished(TaskReduce) {
		log.Println("all reduce tasks assigned but not finished")
		reply.TaskType = TaskEmpty
		return nil
	}
	//任务结束
	reply.TaskType = TaskDone
	return nil
}

func (c *Coordinator) isTaskFinished(taskType TaskType) bool {
	for _, task := range c.tasks {
		if task.t == taskType && !task.isFinished() {
			return false
		}
	}
	return true
}

func (c *Coordinator) getAvailableTask(taskType TaskType) *taskInfo {
	for _, task := range c.tasks {
		if task.t == taskType && task.isAssignable() {
			return task
		}
	}
	return nil
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	log.Printf("FinishTaskArgs: %v ", args)
	for _, task := range c.tasks {
		if task.t == args.TaskType && task.no == args.TaskNo {
			task.finish()
			break
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	//sockname := coordinatorSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	log.Printf("----task status-----")
	for _, task := range c.tasks {
		log.Printf("%v, %v, %v\n", task.t, task.no, task.status)
	}
	ret := c.isTaskFinished(TaskReduce)
	return ret
}

func (c *Coordinator) Timeout() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, task := range c.tasks {
		if task.isExpired() {
			task.timeout()
		}
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	tasks := make([]*taskInfo, len(files)+nReduce)
	for i := 0; i < len(files); i++ {
		tasks[i] = &taskInfo{
			t:        TaskMap,
			filename: files[i],
			no:       i,
		}
	}

	for i := 0; i < nReduce; i++ {
		tasks[len(files)+i] = &taskInfo{
			t:  TaskReduce,
			no: i,
		}
	}

	c := Coordinator{
		files:   files,
		nReduce: nReduce,
		mutex:   &sync.Mutex{},
		tasks:   tasks,
	}

	ticker := time.Tick(time.Second)
	go func() {
		for range ticker {
			c.Timeout()
		}
	}()

	// Your code here.

	c.server()
	return &c
}
