package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.
	nMap	int
	nReduce	int
	reduceTasks	[]Task
	mapTasks	[]Task
	mu	sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetReduceCount(args *GetReduceCountArgs, reply *GetReduceCountReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.ReduceCount = len(c.reduceTasks)

	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error{
	c.mu.Lock()

	var task *Task
	if c.nMap > 0 {
		task = c.selectTask(c.mapTasks, args.WorkerId)
	} else if c.nReduce > 0 {
		task = c.selectTask(c.reduceTasks, args.WorkerId)
	} else {
		task = &Task{ExitTask, Finished, -1, "", -1}
	}

	reply.TaskType = task.Type
	reply.TaskId = task.Index
	reply.TaskFile = task.File

	// fmt.Println("RequestTask: selected task: ", *task)
	c.mu.Unlock()
	go c.waitForTask(task)
	return nil
}

func (c *Coordinator) ReportTaskDone(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var task *Task
	if args.TaskType == MapTask {
		task = &c.mapTasks[args.TaskId]
	} else if args.TaskType == ReduceTask {
		task = &c.reduceTasks[args.TaskId]
	} else {
		fmt.Printf("Incorrect task type to report: %v\n", args.TaskType)
		return nil
	}

	// workers can only report task done if the task was not re-assigned due to timeout
	if args.WorkerId == task.WorkerId && task.Status == Executing {
		// fmt.Printf("Task %v reports done.\n", *task)
		task.Status = Finished
		if args.TaskType == MapTask && c.nMap > 0 {
			c.nMap--
		} else if args.TaskType == ReduceTask && c.nReduce > 0 {
			c.nReduce--
		}
	}

	reply.CanExit = c.nMap == 0 && c.nReduce == 0

	return nil
}



//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	nMap := len(files)
	c.nMap = nMap
	c.nReduce = nReduce
	c.mapTasks = make([]Task, 0, nMap)
	c.reduceTasks = make([]Task, 0, nMap)

	for i := 0; i<nMap; i++ {
		mTask := Task{MapTask, NotStarted, i, files[i], -1}
		m.mapTasks = append(c.mapTasks, mTask)
	}

	for i := 0; i<nReduce; i++ {
		mTask := Task{ReduceTask, NotStarted, i, files[i], -1}
		c.reduceTasks = append(c.reduceTasks, mTask)
	}

	c.server()



	// Your code here.


	c.server()
	return &c
}
