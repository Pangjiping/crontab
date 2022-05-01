package main

import (
	"flag"
	"fmt"
	"github.com/Pangjiping/crontab/worker"
	"runtime"
	"time"
)

var (
	configFile string //配置文件路径
)

// 解析命令行参数
func initArgs() {
	// work -config ./master.json
	flag.StringVar(&configFile, "config", "./worker.json", "指定worker.json")
	flag.Parse()
}

// 初始化线程数量
func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	var (
		err error
	)
	// 初始化命令行参数
	initArgs()

	// 初始化线程
	initEnv()

	// 加载配置
	err = worker.InitConfig(configFile)
	if err != nil {
		goto ERR
	}

	// 服务注册
	err = worker.InitRegister()
	if err != nil {
		goto ERR
	}

	// 启动日志协程
	err = worker.InitLogSink()
	if err != nil {
		goto ERR
	}

	// 启动执行器
	err = worker.InitExecutor()
	if err != nil {
		goto ERR
	}

	// 启动调度器
	err = worker.InitScheduler()
	if err != nil {
		goto ERR
	}

	// 初始化任务管理器
	err = worker.InitJobMgr()
	if err != nil {
		goto ERR
	}

	time.Sleep(time.Minute * 10)
	return
ERR:
	fmt.Println(err)
}
