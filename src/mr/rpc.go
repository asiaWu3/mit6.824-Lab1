package mr

//
// RPC定义

import "os"
import "strconv"

// 要任务时携带的信息
type TaskArgs struct {
}

// 要任务时master返回的信息
type TaskReply struct {
	Task    *Task //分配的任务
	NReduce int   //reduce任务的数量
	AllDone bool  //master中的所有任务是否已经完成
}

// mapWorker通知master任务完成时携带的信息
type MapTaskDoneArgs struct {
	MapWorkerId int
	Files       []string
}

// mapWorker通知master任务完成时master返回的信息
type MapTaskDoneReply struct {
}

// reduceWorker通知master任务完成时携带的信息
type ReduceTaskDoneArgs struct {
	ReduceWorkerId int
	Files          []string
}

// reduceWorker通知master任务完成时master返回的信息
type ReduceTaskDoneReply struct {
}

type TaskType int        //任务类型 MapTask为0 ReduceTask为1
type DistributePhase int //任务分配的阶段
type TaskStatus int      //任务进行阶段
const (
	//MapTask和ReduceTask用于区分任务类型
	MapTask    = 0
	ReduceTask = 1
	//MapPhase和ReducePhase用于master任务分配的阶段 MapPhase表示正在分配Map任务
	MapPhase    = 0
	ReducePhase = 1
	//任务进行阶段
	Ready    = 0
	Running  = 1
	Finished = 2
)

// 在/var/tmp中为主进程创建一个独特的UNIX域套接字名称，不能使用当前目录，因为Athena AFS不支持UNIX域套接字
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
