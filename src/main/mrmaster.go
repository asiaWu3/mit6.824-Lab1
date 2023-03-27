package main

import "project/6.824/src/mr"
import "time"
import "os"
import "fmt"

func main() {
	//判断命令行启动参数个数是否符合要求
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}
	//创建一个master，传入要处理的文件名和reduce任务的个数
	m := mr.MakeMaster(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
