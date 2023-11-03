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
type Task struct {
	TaskType   TaskType // 任务类型map或者reduce
	TaskId     int      // 任务id
	ReducerNum int      // 传入reducer的数量
	FileSlice  []string // 输入文件的slice，map处理一个文件对应一个文件，reduce对应多个temp中间值文件
}
type TaskArgs struct{}

type TaskType int

// 任务类型
const (
	MapTask TaskType = iota // Map人物
	ReduceTask 	// Reduce任务
	WaitingTask // WaitingTask代表此时任务未完成（Map或Reduce任务分发已完成），未进入下一个阶段
	ExitTask    // 结束退出
)

type Phase int

// 阶段类型
const (
	MapPhase    Phase = iota // 分发MapTask
	ReducePhase              // 分发ReduceTask
	AllDone                  // 已完成
)

type State int

// 任务状态类型
const (
	Working State = iota // 工作中
	Waiting              // 等待执行
	Done                 // 已完成
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
