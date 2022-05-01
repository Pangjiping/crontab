package worker

import (
	"fmt"
	"math/rand"
	"os/exec"
	"time"

	"github.com/Pangjiping/crontab/common"
)

// 任务执行器
type Executor struct {
}

var (
	G_executor *Executor
)

// ExecuteJob 执行一个任务
func (executor *Executor) ExecuteJob(info *common.JobExecuteInfo) {
	go func() {
		result := &common.JobExecuteResult{
			ExecuteInfo: info,
			Output:      make([]byte, 0),
		}

		// 初始化锁
		jobLock := G_jobMgr.CreateJobLock(info.Job.Name)

		// 记录任务开始事件
		result.StartTime = time.Now()

		// 随机睡眠(0-1s)
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

		// 上锁
		err := jobLock.TryLock()
		defer jobLock.Unlock()

		// 如果上锁失败
		if err != nil {
			result.Err = err
			result.EndTime = time.Now()
			G_scheduler.PushJobResult(result)
			return
		}

		// 如果上锁成功
		// 重置任务启动时间
		result.StartTime = time.Now()

		// 执行shell命令
		cmd := exec.CommandContext(info.CancelCtx, "/bin/bash", "-c", info.Job.Command)
		// 执行并捕获输出
		output, err := cmd.CombinedOutput()

		// 记录任务结束时间
		result.EndTime = time.Now()
		result.Output = output
		result.Err = err

		// 把执行的结果返回给scheduler，scheduler会从executingTable中删除掉执行记录
		G_scheduler.PushJobResult(result)

		fmt.Println("任务执行完成：", result.ExecuteInfo.Job.Name, string(result.Output), result.Err)
	}()
}

// InitExecutor 初始化执行器
func InitExecutor() error {
	G_executor = &Executor{}
	return nil
}
