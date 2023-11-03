package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import (
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 处理多个worker访问协调者的锁
var (
	mu sync.Mutex
)

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int        // 传入的参数决定需要多少个reducer
	TaskId            int        // 用于生成task的特殊id
	DistPhase         Phase      // 目前整个体系处于的任务阶段
	ReduceTaskChannel chan *Task // 使用go管道，天然并发安全
	MapTaskChannel    chan *Task
	taskInfoMap       TaskInfoMap // 存放task全部元数据的映射
	files             []string    // 传入的文件数组
}

// 定义一个struct保存Task的元数据
type TaskInfo struct {
	state     State     // 任务状态
	StartTime time.Time // 任务的开始时间，用于处理crash
	TaskAdr   *Task     // 传入任务对应的指针，这样一来任务出管道后还能通过指针修改其任务状态
}

// 用一个映射保存全部Task的元数据
type TaskInfoMap struct {
	TaskMap map[int]*TaskInfo // 通过下标hash快速定位
}

// 定义一个方法，用于将元数据存入元数据映射集合
func (t *TaskInfoMap) acceptTaskInfo(TaskInfo *TaskInfo) bool {
	taskId := TaskInfo.TaskAdr.TaskId
	info, _ := t.TaskMap[taskId]
	if info != nil {
		//fmt.Println("Holder ALREADY contains task：", taskId)
		return false
	} else {
		t.TaskMap[taskId] = TaskInfo
	}
	return true
}

// 通过struct的任务id递增，获得对应的任务id
func (c *Coordinator) generateTaskId() int {
	res := c.TaskId
	c.TaskId++
	return res
}

// 对map任务进行处理，将Map任务放进管道，将taskInfo保存到taskInfoMap
func (c *Coordinator) makeMapTasks(files []string) {
	for _, v := range files {
		id := c.generateTaskId()
		task := Task{
			TaskType:   MapTask,
			TaskId:     id,
			ReducerNum: c.ReducerNum,
			FileSlice:  []string{v},
		}
		// 保存任务的初始状态
		taskInfo := TaskInfo{
			state:   Waiting, // 任务等待被执行
			TaskAdr: &task,   // 保存任务的地址
		}
		c.taskInfoMap.acceptTaskInfo(&taskInfo)
		c.MapTaskChannel <- &task
	}
}

// 对reduce任务进行处理，将任务放进管道，将taskInfo保存到taskInfoMap
func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateTaskId()
		task := Task{
			TaskType:  ReduceTask,
			TaskId:    id,
			FileSlice: selectReduceName(i),
		}
		// 保存任务的初始状态
		taskInfo := TaskInfo{
			state:   Waiting, // 任务等待被执行
			TaskAdr: &task,   // 保存任务的地址
		}
		c.taskInfoMap.acceptTaskInfo(&taskInfo)
		c.ReduceTaskChannel <- &task
	}
}

// makeReduceTasks中通过哈希值匹配中间文件的方法
func selectReduceName(reduceNum int) []string {
	var selected []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		// 通过哈希值匹配对应的reduce mr-tmp文件
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceNum)) {
			selected = append(selected, fi.Name())
		}
	}
	return selected
}

// 分发任务，将map任务管道中的任务取出
// 判断任务map任务是否先完成，如果完成那么应该进入下一个任务处理阶段（ReducePhase）
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	// 上锁防止worker访问协调者冲突，一个worker访问结束后最后解锁
	mu.Lock()
	defer mu.Unlock()
	// 按任务类型处理
	switch c.DistPhase {
	case MapPhase:
		{
			if len(c.MapTaskChannel) > 0 {
				*reply = *<-c.MapTaskChannel
				//fmt.Printf("poll map taskid[%d]\n", reply.TaskId)
				if !c.taskInfoMap.judgeState(reply.TaskId) {
					fmt.Printf("Maptask[%d] is on working\n", reply.TaskId)
				}
			} else { // 如果map任务被分发完但未完成，此时就将任务设为Waiting
				reply.TaskType = WaitingTask
				if c.taskInfoMap.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case ReducePhase:
		{ // 同上
			if len(c.ReduceTaskChannel) > 0 {
				*reply = *<-c.ReduceTaskChannel
				//fmt.Printf("poll Reduce taskid[%d]\n", reply.TaskId)
				if !c.taskInfoMap.judgeState(reply.TaskId) {
					fmt.Printf("Reducetask[%d] is running\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaitingTask
				if c.taskInfoMap.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	case AllDone:
		{

			reply.TaskType = ExitTask
		}
	default:
		panic("error: Wrong phase!")
	}
	return nil
}

// PollTask调用的切换phase的方法
func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase {
		c.makeReduceTasks()
		c.DistPhase = ReducePhase
	} else if c.DistPhase == ReducePhase {
		c.DistPhase = AllDone
	}
}

// PollTask调用的返回任务完成情况的方法
func (t *TaskInfoMap) checkTaskDone() bool {
	var (
		mapDoneNum      = 0
		mapUnDoneNum    = 0
		reduceDoneNum   = 0
		reduceUnDoneNum = 0
	)
	// 遍历储存task信息的映射
	for _, v := range t.TaskMap {
		if v.TaskAdr.TaskType == MapTask { // Map任务的类型
			if v.state == Done { // Map任务是否完成
				mapDoneNum++
			} else {
				mapUnDoneNum++
			}
		} else if v.TaskAdr.TaskType == ReduceTask { // Reduce任务，同上
			if v.state == Done {
				reduceDoneNum++
			} else {
				reduceUnDoneNum++
			}
		}
	}
	// 判断该阶段是否完成，返回true or false
	if (mapDoneNum > 0 && mapUnDoneNum == 0) && (reduceDoneNum == 0 && reduceUnDoneNum == 0) {
		return true
	} else {
		if reduceDoneNum > 0 && reduceUnDoneNum == 0 {
			return true
		}
	}
	return false
}

// PollTask调用的判断任务工作状态的方法，协助判定是否crash
func (t *TaskInfoMap) judgeState(taskId int) bool {
	taskInfo, ok := t.TaskMap[taskId]
	// 判断该id的任务是否在工作，不在工作则返回false
	if !ok || taskInfo.state != Waiting {
		return false
	}
	// 如果不在工作则改为工作并返回true
	taskInfo.state = Working
	taskInfo.StartTime = time.Now()
	return true
}

// 调用的rpc方法，将任务标记为已完成
func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.TaskType {
	case MapTask:
		info, ok := c.taskInfoMap.TaskMap[args.TaskId]
		if ok && info.state == Working {
			info.state = Done
		} else {
			fmt.Printf("Maptask[%d] is finished\n", args.TaskId)
		}
		break
	case ReduceTask:
		info, ok := c.taskInfoMap.TaskMap[args.TaskId]
		if ok && info.state == Working {
			info.state = Done
		} else {
			fmt.Printf("Reducetask[%d] is finished\n", args.TaskId)
		}
		break
	default:
		panic("ERROR: Wrong tasktype!")
	}
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
//
// Done 主函数mr调用，如果所有task完成mr会通过此方法退出
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	if c.DistPhase == AllDone {
		// sleep防止early exit
		time.Sleep(3 * time.Second)
		//fmt.Printf("All tasks finished, the coordinator will close.")
		return true
	} else {
		return false
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		ReducerNum:        nReduce,
		DistPhase:         MapPhase,
		MapTaskChannel:    make(chan *Task, len(files)),
		ReduceTaskChannel: make(chan *Task, nReduce),
		taskInfoMap: TaskInfoMap{
			TaskMap: make(map[int]*TaskInfo, len(files)+nReduce),
		},
	}
	c.makeMapTasks(files)
	c.server()
	go c.CrashSolver()

	return &c
}

// 处理crash的方法
func (c *Coordinator) CrashSolver() {
	for {
		// 等待系统启动完毕，上锁
		time.Sleep(time.Second * 3)
		mu.Lock()
		// 所有任务完成后退出
		if c.DistPhase == AllDone {
			mu.Unlock()
			break
		}
		// 侦测各个worker处于working状态的时间，超过10秒的将其放回管道
		for _, v := range c.taskInfoMap.TaskMap {
			if v.state == Working && time.Since(v.StartTime) > 10*time.Second {
				//fmt.Printf("task[%d] is crashed, has worked %d time\n", v.TaskAdr.TaskId, time.Since(v.StartTime))
				// 通过地址将超时任务放回管道
				switch v.TaskAdr.TaskType {
				case MapTask:
					c.MapTaskChannel <- v.TaskAdr
					v.state = Waiting
				case ReduceTask:
					c.ReduceTaskChannel <- v.TaskAdr
					v.state = Waiting
				}
			}
		}
		mu.Unlock()
	}

}
