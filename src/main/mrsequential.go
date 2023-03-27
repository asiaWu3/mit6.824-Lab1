package main

//
// 串行的MapReduce示例
//
// 运行命令：go run mrsequential.go wc.so pg*.txt
//

import (
	"fmt"
	"io"
	"plugin"
)
import "project/6.824/src/mr"
import "os"
import "log"
import "sort"

// 20-24行的定义用于根据key排序
type ByKey []mr.KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main() {
	//判断命令行启动参数个数是否符合要求
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		os.Exit(1)
	}
	//动态加载插件，把map和reduce函数加载为mapf和reducef
	mapf, reducef := loadPlugin(os.Args[1])

	//读取每个输入文件并依次传递给Map函数，累加每个Map函数返回的结果到intermediate中
	intermediate := []mr.KeyValue{}
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		//读取file文件全部内容
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	//与真正的MapReduce相比，一个很大的区别是所有中间数据都在一个地方，即intermediate []，而不是被分割成NxM个桶。

	//将intermediate按key字典序排序
	sort.Sort(ByKey(intermediate))

	//创建输出文件mr-out-0
	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//对于intermediate[]中的每个不同的键，调用Reduce函数，并将结果打印到mr-out-0中
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

// 加载插件，比如： ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
