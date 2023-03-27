package main

import "project/6.824/src/mr"
import "plugin"
import "os"
import "fmt"
import "log"

func main() {
	//判断命令行启动参数个数是否符合要求
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}
	//动态加载插件，把map和reduce函数加载为mapf和reducef
	mapf, reducef := loadPlugin(os.Args[1])
	//创建worker进程
	mr.Worker(mapf, reducef)
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
// 加载Map和Reduce函数并返回
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
