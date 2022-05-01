package main

import (
	"flag"
	"fmt"
	"runtime"
	"time"

	"github.com/Pangjiping/crontab/master"
)

var (
	configFile string //配置文件路径
)

// initArgs 初始化命令行参数
func initArgs() {
	// master -config ./master.json
	flag.StringVar(&configFile, "config", "./master.json", "指定master.json")
	flag.Parse()
}

// initEnv 初始化线程数量
func initEnv() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func main() {
	var err error

	initArgs()
	initEnv()

	err = master.InitConfig(configFile)
	if err != nil {
		goto ERR
	}

	err = master.InitWorkerMgr()
	if err != nil {
		goto ERR
	}

	err = master.InitLogMgr()
	if err != nil {
		goto ERR
	}

	err = master.InitJobMgr()
	if err != nil {
		goto ERR
	}

	err = master.InitApiServer()
	if err != nil {
		goto ERR
	}

	time.Sleep(time.Minute * 1000)
	return
ERR:
	fmt.Println(err)
}
