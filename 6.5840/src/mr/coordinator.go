package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "sync"
import "time"

type JobState int

const (
	Mapping JobState = iota
	Reducing
	Done
)

type Coordinator struct {
	// Your definitions here.
	State 		JobState
	NReduce		int
	MapTasks	[]*MapTask
	ReduceTasks	[]*ReduceTask

	MappedTaskId map[int]struct{}
	MaxTaskId	 int
	Mutex		 sync.Mutex

	WorkerCount	 int
	ExcitedCount int
}

// Your code here -- RPC handlers for the worker to call.

const TIMEOUT = 10 * time.Second

func (c *Coordinator) RequestTask(_ *Placeholder, reply *Task) error{
	reply.Operation = ToWait

	if c.State == Mapping {
		for _, task := range c.MapTasks {
			now := time.Now()
			c.Mutex.Lock()
			if task.State == Executing && task.StartTime.Add(TIMEOUT).Before(now) {
				task.State = Pending
			}
			if task.State == Pending {
				task.StartTime = now
				task.State = Executing
				c.MaxTaskId++
				task.Id = c.MaxTaskId
				c.Mutex.Unlock()
				log.Printf("assigned map task %d %s", task.Id, task.FileName)

				reply.Operation = ToRun
				reply.IsMap = true
				reply.NReduce = c.NReduce
				reply.Map = *task
				return nil
			}
			c.Mutex.Unlock()
		}
	} else if c.State == Reducing {
		for _, task := range c.ReduceTasks {
			now := time.Now()
			c.Mutex.Lock()
			if task.State == Executing && task.StartTime.Add(TIMEOUT).Before(now) {
				task.State = Pending
			}
			if task.State == Pending {
				task.StartTime = now
				task.State = Executing
				task.IntermediateFilenames = nil
				for id := range c.MappedTaskId {
					task.IntermediateFilenames = append(task.IntermediateFilenames, fmt.Sprintf("mr-%d-%d", id, task.Id))
				}
				c.Mutex.Unlock()
				log.Printf("assigned reduce task %d", task.Id)

				reply.Operation = ToRun
				reply.IsMap = false
				reply.NReduce = c.NReduce
				reply.Reduce = *task
				return nil
			}
			c.Mutex.Unlock()
		}
	}
	return nil
}

func (c *Coordinator) Finish(args *FinishArgs, _ *Placeholder) error {
	if args.IsMap {
		for _, task := range c.MapTasks {
			if task.Id == args.Id {
				task.State = Finished
				log.Printf("finished task %d, total %d", task.Id, len(c.MapTasks))
				c.MappedTaskId[task.Id] = struct{}{}
				break
			}
		}
		// 
		for _, t := range c.MapTasks {
			if t.State != Finished {
				return nil
			}
		}
		c.State = Reducing
	} else {
		for _, task := range c.ReduceTasks {
			if task.Id == args.Id {
				task.State = Finished
				break
			}
		}
		for _, t := range c.ReduceTasks {
			if t.State != Finished {
				return nil
			}
		}
		c.State = Done
	}
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
	// ret := false

	// Your code here.

	return c.State == Done
	// return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NReduce: 		nReduce,
		MaxTaskId:		0,
		MappedTaskId: 	make(map[int]struct{}),
	}

	// Your code here.
	for _, f := range files {
		c.MapTasks = append(c.MapTasks, &MapTask{TaskMeta: TaskMeta{State: Pending}, FileName: f})
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTasks = append(c.ReduceTasks, &ReduceTask{TaskMeta: TaskMeta{State: Pending, Id: i}})
	}

	c.State = Mapping
	c.server()
	return &c
}
