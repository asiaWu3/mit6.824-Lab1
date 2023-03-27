package mr

import (
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// 定义master
type Master struct {
	MapChannel      chan *Task      //存储所有的map任务
	ReduceChannel   chan *Task      //存储所有的reduce任务
	Files           []string        //map任务要处理的所有文件
	MapNum          int             //map任务的数量
	ReduceNum       int             //reduce任务的数量
	DistributePhase DistributePhase //当前任务分配的阶段，包括：MapPhase、ReduceMapPhase
	Lock            sync.Mutex      //锁
	WorkerId        int             //worker的唯一标识
	IsDone          bool            //所有任务是否完成
}

// 定义任务Task
type Task struct {
	TaskType       TaskType   //任务类型，包括MapTask和ReduceTask
	MapWorkerId    int        //如果是map任务，存储mapWorker的id
	ReduceWorkerId int        //如果是reduce任务，存储reduceWorker的id
	InputFile      string     //该任务要处理的文件路径
	BeginTime      time.Time  //任务创建时间
	TaskStatus     TaskStatus //任务状态，包括Ready、Running、Finished
}

// 开启一个线程监听来自worker.go的RPC调用
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// 该方法由mrmaster.go调用，用于判断是否已经完成了全部工作
func (m *Master) Done() bool {
	m.Lock.Lock()
	isDone := m.IsDone
	m.Lock.Unlock()
	return isDone
}

// 该方法由mrmaster.go调用，用于创建一个Master
// nReduce是reduce任务数量
func MakeMaster(files []string, nReduce int) *Master {
	log.Println("创建Master")
	m := Master{
		MapChannel:      make(chan *Task, len(files)), //map任务数量和要处理文件的数量一致
		ReduceChannel:   make(chan *Task, nReduce),    //reduce任务数量为nReduce
		Files:           files,
		MapNum:          len(files),
		ReduceNum:       nReduce,
		DistributePhase: MapPhase, //master刚创建时处于分配map任务阶段
		WorkerId:        1,        //workerId初始为1
	}
	//根据要处理的文件生成所有map任务
	m.generateMapTasks(files)
	// 循环移除过期（10s）map、reduce任务
	go m.loopRemoveTimeoutTasks()
	//开始监听rpc调用
	m.server()
	log.Println("开始监听")
	return &m
}

// 循环移除过期任务
func (m *Master) loopRemoveTimeoutTasks() {
	for !m.Done() {
		time.Sleep(time.Second)
		m.Lock.Lock()
		if m.DistributePhase == MapPhase {
			for i := 0; i < m.MapNum; i++ {
				task := <-m.MapChannel
				m.MapChannel <- task
				m.removeTimeoutTask(task)
				//m.Lock.Unlock()
			}
		} else {
			for i := 0; i < m.ReduceNum; i++ {
				//m.Lock.Lock()
				task := <-m.ReduceChannel
				m.ReduceChannel <- task
				m.removeTimeoutTask(task)
			}
		}
		m.Lock.Unlock()
	}
}

// 判断task任务是否过期，过期则移除
func (m *Master) removeTimeoutTask(task *Task) {
	if task.TaskStatus == Running && (time.Now().Sub(task.BeginTime) >= 10*time.Second) {
		log.Println("map任务过期:", task)
		//过期后将任务状态重新设为Ready
		task.TaskStatus = Ready
	}
}

// 根据要处理的文件生成所有map任务
func (m *Master) generateMapTasks(files []string) {
	//清除上次运行留下的文件，防止干扰
	oldFiles, _ := filepath.Glob(TempFilePath + "mr-*")
	for _, oldFile := range oldFiles {
		os.Remove(oldFile)
	}
	log.Println("开始生成map任务")
	m.Lock.Lock()
	for _, file := range files {
		task := Task{
			TaskType:   MapTask,
			InputFile:  file,
			TaskStatus: Ready, //初始时任务状态为Ready
		}
		log.Println("生成map任务", ":", task)
		m.MapChannel <- &task
	}
	m.Lock.Unlock()
	log.Println("map任务生成完成")
}

// 所有map任务完成后生成reduce任务
func (m *Master) generateReduceTasks() {
	log.Println("开始生成reduce任务")
	m.Lock.Lock()
	for i := 0; i < m.ReduceNum; i++ {
		task := Task{
			TaskType:   ReduceTask,
			InputFile:  fmt.Sprintf("%vmr-*-%v", TempFilePath, i),
			TaskStatus: Ready,
		}
		log.Println("生成reduce任务", i, ":", task)
		m.ReduceChannel <- &task
	}
	m.Lock.Unlock()
	log.Println("reduce任务生成完成")
}

// 该方法由worker通过rpc调用，每次调用会分配给worker一个map或reduce任务
func (m *Master) DistributeTask(args *TaskArgs, reply *TaskReply) error {
	//处于map分配阶段就分配map任务
	if m.DistributePhase == MapPhase {
		m.Lock.Lock()
		for i := 0; i < m.MapNum; i++ {
			task := <-m.MapChannel
			m.MapChannel <- task
			//分配处于Ready状态的map任务
			if task.TaskStatus == Ready {
				task.MapWorkerId = m.WorkerId
				m.WorkerId++
				//将任务状态设为Running
				task.TaskStatus = Running
				task.BeginTime = time.Now()
				//把map任务和reduce任务的数量返回
				reply.Task = task
				reply.NReduce = m.ReduceNum
				log.Println("已分配map任务：", task)
				m.Lock.Unlock()
				return nil
			}
			//m.Lock.Unlock()
		}
		m.Lock.Unlock()
		//如果上面的循环中没有找到处于Ready状态的map任务，说明map任务已经全部分配出去，判断map任务是否已经全部完成
		if m.judgeAllMapDone() {
			log.Println("map任务全部完成，即将生成reduce任务")
			//生成所有reduce任务
			m.generateReduceTasks()
			//将分配阶段改为reduce阶段
			m.DistributePhase = ReducePhase
		} else {
			//map任务全部分配，但暂未全部完成
			time.Sleep(time.Second)
		}
		return nil
	} else { //处于reduce分配阶段
		m.Lock.Lock()
		for i := 0; i < m.ReduceNum; i++ {
			task := <-m.ReduceChannel
			m.ReduceChannel <- task
			if task.TaskStatus == Ready {
				task.ReduceWorkerId = m.WorkerId
				m.WorkerId++
				task.TaskStatus = Running
				task.BeginTime = time.Now()
				reply.Task = task
				log.Println("已分配reduce任务：", task)
				m.Lock.Unlock()
				return nil
			}
		}
		m.Lock.Unlock()
		//如果上面的循环中没有找到处于Ready状态的reduce任务，说明reduce任务已经全部分配出去，判断reduce任务是否已经全部完成
		if m.judgeAllReduceDone() {
			log.Println("reduce任务全部完成")
			//全部完成后把AllDone设为true并调用finish方法善后
			reply.AllDone = true
			go m.finish()
		} else {
			//reduce任务全部分配，但暂未全部完成
			time.Sleep(time.Second)
		}
	}
	return nil
}

// worker的map任务完成时通过rpc调用该方法
func (m *Master) MapTaskDone(args *MapTaskDoneArgs, reply *MapTaskDoneReply) error {
	mapWorkerId := args.MapWorkerId
	//判断之前该任务是否已被回收
	flag := false
	m.Lock.Lock()
	for i := 0; i < m.MapNum; i++ {
		task := <-m.MapChannel
		m.MapChannel <- task
		//再判断一下任务是否超时
		m.removeTimeoutTask(task)
		//如果某任务的workerId和rpc调用的workerId一致并且该任务的状态没有被重置为Ready，也没有被其他worker完成
		if task.MapWorkerId == mapWorkerId && task.TaskStatus == Running {
			task.TaskStatus = Finished
			log.Printf("map任务：%v 在规定时间内顺利完成\n", mapWorkerId)
			//生成map中间文件
			m.generateMapFile(args.Files)
			flag = true
			//m.Lock.Unlock()
			break
		}
	}
	m.Lock.Unlock()
	if !flag {
		log.Printf("map任务：%v 未在规定时间内完成\n", mapWorkerId)
	}
	return nil
}

// worker的reduce任务完成时通过rpc调用该方法
func (m *Master) ReduceTaskDone(args *ReduceTaskDoneArgs, reply *ReduceTaskDoneReply) error {
	reduceWorkerId := args.ReduceWorkerId
	//判断之前该任务是否已被回收
	flag := false
	m.Lock.Lock()
	for i := 0; i < m.ReduceNum; i++ {
		task := <-m.ReduceChannel
		m.ReduceChannel <- task
		if task.ReduceWorkerId == reduceWorkerId && task.TaskStatus == Running {
			task.TaskStatus = Finished
			log.Printf("map任务：%v 在规定时间内顺利完成\n", reduceWorkerId)
			m.generateReduceFile(args.Files)
			flag = true
			//m.Lock.Unlock()
			break
		}
	}
	m.Lock.Unlock()
	if !flag {
		log.Printf("map任务：%v 未在规定时间内完成\n", reduceWorkerId)
	}
	return nil
}

// 判断map任务是否全部完成
func (m *Master) judgeAllMapDone() bool {
	m.Lock.Lock()
	for i := 0; i < m.MapNum; i++ {
		task := <-m.MapChannel
		m.MapChannel <- task
		if task.TaskStatus != Finished {
			m.Lock.Unlock()
			return false
		}
	}
	m.Lock.Unlock()
	return true
}

// 判断reduce任务是否全部完成
func (m *Master) judgeAllReduceDone() bool {
	m.Lock.Lock()
	for i := 0; i < m.ReduceNum; i++ {
		task := <-m.ReduceChannel
		m.ReduceChannel <- task
		if task.TaskStatus != Finished {
			m.Lock.Unlock()
			return false
		}
	}
	m.Lock.Unlock()
	return true
}

// 生成map中间文件，实际上是把mapWorker生成的临时文件复制到当前目录，表示接受worker的工作成果
func (m *Master) generateMapFile(files []string) {
	log.Println("生成map中间文件")
	for _, file := range files {
		src, err := os.Open(file)
		if err != nil {
			log.Fatalln(err)
		}
		defer src.Close()
		// 创建目标文件
		//dir为当前运行目录
		dir, _ := os.Getwd()
		//filename为文件名
		filename := filepath.Base(file)
		filepath := dir + "/" + filename
		dst, err := os.Create(filepath)
		if err != nil {
			log.Fatalln(err)
		}
		defer dst.Close()
		// 复制文件内容
		_, err = io.Copy(dst, src)
		if err != nil {
			log.Fatalln(err)
		}
	}
	log.Println("map中间文件生成完成")
}

// 生成reduce最终文件，实际上是把reduceWorker生成的临时文件复制到当前目录，表示接受worker的工作成果
func (m *Master) generateReduceFile(files []string) {
	log.Println("生成reduce最终文件")
	for _, file := range files {
		src, err := os.Open(file)
		if err != nil {
			log.Fatalln(err)
		}
		defer src.Close()
		// 创建目标文件
		dir, _ := os.Getwd()
		filename := filepath.Base(file)
		filepath := dir + "/" + filename
		dst, err := os.Create(filepath)
		if err != nil {
			log.Fatalln(err)
		}
		defer dst.Close()
		// 复制文件内容
		_, err = io.Copy(dst, src)
		if err != nil {
			log.Fatalln(err)
		}
	}
	log.Println("reduce最终文件生成完成")
}

// 全部任务完成
func (m *Master) finish() {
	//等三秒的作用是留一段时间告诉接下来请求任务的worker们所有任务已经完成了，让它们（worker们）结束运行
	time.Sleep(time.Second * 3)
	m.Lock.Lock()
	m.IsDone = true
	m.Lock.Unlock()
}
