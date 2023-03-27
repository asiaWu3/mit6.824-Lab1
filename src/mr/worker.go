package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
)

// Map方法返回KeyValue的一个切片
type KeyValue struct {
	Key   string
	Value string
}

// 定义worker
type AWorker struct {
	WorkerId int                             //workerId
	Mapf     func(string, string) []KeyValue //加载进来的map方法
	Reducef  func(string, []string) string   //加载进来的reduce方法
	Task     *Task                           //该worker要完成的任务
	NReduce  int                             //reduce任务的数量
	IsDone   bool                            //是否可以退出程序
}

// 31-36行的定义用于根据key排序
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

const (
	//临时文件的存储路径
	TempFilePath = "/var/tmp/"
)

// 将键值对通过 Map 返回后，使用 ihash(key) % NReduce 来选择每个 KeyValue 被分配到哪个 Reduce 任务编号
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// 该方法由mrworker.go调用，用于初始化一个worker
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	log.Println("初始化worker")
	worker := AWorker{
		WorkerId: -1, //未分配时为-1
		Mapf:     mapf,
		Reducef:  reducef,
		IsDone:   false,
	}
	log.Println("初始化worker完成")
	for !worker.IsDone {
		//只要master没告诉worker所有任务都完成了，worker就一直work请求新的任务
		worker.work()
	}
	log.Println("worker", worker.WorkerId, "本次任务完成")
}

func (worker *AWorker) work() {
	task := worker.askTask()
	if task != nil {
		log.Println("请求任务成功:", task)
		worker.Task = task
		if task.TaskType == MapTask {
			worker.WorkerId = task.MapWorkerId
			worker.doMap(worker.Mapf)
		} else {
			worker.WorkerId = task.ReduceWorkerId
			worker.doReduce(worker.Reducef)
		}
	} else {
		log.Println("请求任务失败")
		if !worker.IsDone {
			log.Println("重新请求任务")
			worker.work()
		}
	}
}

// 通过rpc向master要任务
func (worker *AWorker) askTask() *Task {
	args := TaskArgs{}
	reply := TaskReply{}
	log.Println("worker", worker.WorkerId, "正在请求任务")
	call("Master.DistributeTask", &args, &reply)
	if &reply != nil {
		//如果master告诉worker任务已经全部完成，就把isDone设为true
		if reply.AllDone {
			worker.IsDone = true
			return nil
		}
		worker.NReduce = reply.NReduce
		return reply.Task
	} else {
		return nil
	}
}

// 执行map任务
func (worker *AWorker) doMap(mapf func(string, string) []KeyValue) {
	task := worker.Task
	log.Println("worker:", worker.WorkerId, "开始执行map任务:", task)
	//生成map任务结果，intermediate为KeyValue切片
	intermediate := worker.generateMapResult(task.InputFile, mapf)
	//将map结果写入临时文件
	files := worker.writeMapResToTempFiles(intermediate)
	log.Println("worker:", worker.WorkerId, "map任务执行完毕:", task)
	//通过rpc告诉master该任务已经完成
	worker.mapTaskDone(files)
}

// 执行reduce任务
func (worker *AWorker) doReduce(reducef func(string, []string) string) {
	task := worker.Task
	log.Println("worker:", worker.WorkerId, "开始执行reduce任务:", task)
	//生成reduce任务结果，intermediate为KeyValue切片
	intermediate := worker.generateReduceResult(task.InputFile, reducef)
	//将reduce结果写入临时文件
	files := worker.writeReduceResToTempFiles(task, intermediate)
	log.Println("worker:", worker.WorkerId, "reduce任务执行完毕:", task)
	//通过rpc告诉master该任务已经完成
	worker.reduceTaskDone(files)
}

// 调用Mapf方法生成map结果KeyValue切片
func (worker *AWorker) generateMapResult(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	intermediate := make([]KeyValue, 0)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	//Mapf崩溃处理
	defer worker.crashHandel()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	return intermediate
}

// 调用Reducef方法生成reduce结果KeyValue切片
func (worker *AWorker) generateReduceResult(filePattern string, reducef func(string, []string) string) []KeyValue {
	intermediate := make([]KeyValue, 0)
	files, err := filepath.Glob(filePattern)
	log.Println(filePattern, files)
	if err != nil {
		log.Fatalln(err)
	}
	for _, filePath := range files {
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalln(err)
		}
		//读取json格式文件，在Lab1 Note中有示例
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	//下面偷的mrsequential.go
	sort.Sort(ByKey(intermediate))
	res := make([]KeyValue, 0)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := make([]string, 0)
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		//Reducef崩溃处理
		defer worker.crashHandel()
		output := reducef(intermediate[i].Key, values)
		kv := KeyValue{Key: intermediate[i].Key, Value: output}
		res = append(res, kv)
		i = j
	}
	return res
}

// 将map处理结果（KeyValue切片）存进临时文件
func (worker *AWorker) writeMapResToTempFiles(intermediate []KeyValue) []string {
	//临时文件名为mr-X-Y，X为mapWorkerId，即当前worker的id；Y为要分配给的reduce任务的编号
	//相同的单词要分给相同编号的reduce任务，因此要对单词（Key）进行hash运算
	cutRes := make([][]KeyValue, worker.NReduce)
	for i := range cutRes {
		cutRes[i] = make([]KeyValue, 0)
	}
	for _, kv := range intermediate {
		index := ihash(kv.Key) % worker.NReduce
		cutRes[index] = append(cutRes[index], kv)
	}
	files := make([]string, 0)
	for i := range cutRes {
		tempFile, err := ioutil.TempFile(TempFilePath, "mr-")
		if err != nil {
			log.Fatal(err)
		}
		//以json格式存储进临时文件，在Lab1 Note中有示例
		enc := json.NewEncoder(tempFile)
		for _, kv := range cutRes[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal(err)
			}
		}
		tempName := fmt.Sprintf("mr-%v-%v", worker.WorkerId, i)
		err = os.Rename(tempFile.Name(), TempFilePath+tempName)
		files = append(files, TempFilePath+tempName)
		if err != nil {
			log.Fatal(err)
		} else {
			log.Printf("map任务:%v 生成临时文件:%v 成功\n", worker.WorkerId, tempFile)
		}
	}
	return files
}

// 将reduce处理结果（KeyValue切片）存进临时文件
func (worker *AWorker) writeReduceResToTempFiles(task *Task, intermediate []KeyValue) []string {
	tempFile, err := ioutil.TempFile(TempFilePath, "mr-")
	if err != nil {
		log.Fatal(err)
	}
	for _, kv := range intermediate {
		fmt.Fprintf(tempFile, "%v %v\n", kv.Key, kv.Value)
	}
	//index为mr-X-Y中的Y，即要生成的reduce临时文件为mr-out-Y
	index := task.InputFile[len(task.InputFile)-1:]
	tempName := "mr-out-" + index
	err = os.Rename(tempFile.Name(), TempFilePath+tempName)
	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("reduce任务:%v 生成临时文件:%v 成功\n", worker.WorkerId, tempFile)
	}
	return []string{TempFilePath + tempName}
}

// map任务完成，告诉master
func (worker *AWorker) mapTaskDone(files []string) {
	//携带的信息包括当前workerId和生成的临时文件路径
	//由master判断该worker是否在规定时间内完成任务，如果是，就把这些临时文件复制到工作目录
	args := MapTaskDoneArgs{
		MapWorkerId: worker.WorkerId,
		Files:       files,
	}
	reply := MapTaskDoneReply{}
	log.Printf("告诉master map:%v的任务已经全部完成\n", worker.WorkerId)
	call("Master.MapTaskDone", &args, &reply)
}

// reduce任务完成，告诉master
func (worker *AWorker) reduceTaskDone(files []string) {
	//携带的信息包括当前workerId和生成的临时文件路径
	//由master判断该worker是否在规定时间内完成任务，如果是，就把这些临时文件复制到工作目录
	args := ReduceTaskDoneArgs{
		ReduceWorkerId: worker.WorkerId,
		Files:          files,
	}
	reply := ReduceTaskDoneReply{}
	log.Printf("告诉master reduce:%v的任务已经全部完成\n", worker.WorkerId)
	call("Master.ReduceTaskDone", &args, &reply)
	//worker.IsDone = true
}

// 崩溃处理，如果崩溃，就重新work请求新的任务
func (worker AWorker) crashHandel() {
	if r := recover(); r != nil {
		//处理异常情况
		fmt.Println("mapf崩溃，准备重新请求任务", r)
		worker.work()
	}
}

// 用于rpc通信，三个参数分别为：
// rpcname：要调用的master的方法名，通过反射实现
// args：携带的信息
// reply：master返回的信息
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
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
