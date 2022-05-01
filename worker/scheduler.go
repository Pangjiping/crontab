package worker

import (
	"fmt"
	"time"

	"github.com/Pangjiping/crontab/common"
)

// 任务调度
type Scheduler struct {
	jobEventChan      chan *common.JobEvent              // etcd任务事件队列
	jobPlanTable      map[string]*common.JobSchedulePlan //任务调度计划表
	jobExecutingTable map[string]*common.JobExecuteInfo  // 任务执行列表
	jobResultChan     chan *common.JobExecuteResult      // 任务结果队列
}

var (
	G_scheduler *Scheduler
)

// handleJobEvent 处理任务事件
// 主要是根据任务事件类型来处理任务调度计划表和任务执行表
func (scheduler *Scheduler) handleJobEvent(jobEvent *common.JobEvent) {
	var (
		jobSchedulePlan *common.JobSchedulePlan
		err             error
		jobExisted      bool
	)

	// 检查任务事件类型
	switch jobEvent.EventType {

	// 如果是保存任务
	case common.JOB_EVENT_SAVE:
		// 创建任务调度计划
		jobSchedulePlan, err = common.BuildJobSchedulePlan(jobEvent.Job)
		if err != nil {
			return
		}
		// 加入到调度表中
		scheduler.jobPlanTable[jobEvent.Job.Name] = jobSchedulePlan

	// 如果是删除任务
	case common.JOB_EVENT_DELETE:
		// 如果此任务在调度表中，则删除该任务
		if _, jobExisted = scheduler.jobPlanTable[jobEvent.Job.Name]; jobExisted {
			delete(scheduler.jobPlanTable, jobEvent.Job.Name)
		}

	// 如果是强杀任务
	case common.JOB_EVENT_KILL:
		// 取消command执行
		// 判断任务是否在执行中
		if jobExecuteInfo, jobExecuting := scheduler.jobExecutingTable[jobEvent.Job.Name]; jobExecuting {
			jobExecuteInfo.CancelFunc() // 触发command中断杀死shell子进程
			delete(scheduler.jobExecutingTable, jobEvent.Job.Name)
		}
	}
}

// TryStartJob 尝试执行任务
func (scheduler *Scheduler) TryStartJob(jobPlan *common.JobSchedulePlan) {
	// 调度和执行是两个事情

	var (
		jobExecuteInfo *common.JobExecuteInfo
		jobExecuting   bool
	)
	// 执行的任务可能会执行很久，每秒调度一次，但只能执行一次，防止并发！
	// 如果任务正在执行，跳过本次任务
	if _, jobExecuting = scheduler.jobExecutingTable[jobPlan.Job.Name]; jobExecuting {
		fmt.Println("尚未退出，跳过执行:", jobPlan.Job.Name)
		return
	}

	// 构建执行状态信息
	jobExecuteInfo = common.BulidJobExecuteInfo(jobPlan)

	// 保存执行状态
	scheduler.jobExecutingTable[jobPlan.Job.Name] = jobExecuteInfo

	// 执行任务
	G_executor.ExecuteJob(jobExecuteInfo)
}

// TrySchedule 重新计算任务调度状态
func (scheduler *Scheduler) TrySchedule() time.Duration {
	var nearTime *time.Time

	// 如果当前没有任务
	if len(scheduler.jobPlanTable) == 0 {
		return time.Second * 1
	}

	// 当前时间
	now := time.Now()

	// 遍历所有任务
	for _, jobPlan := range scheduler.jobPlanTable {
		// 过期的任务立即执行
		if jobPlan.NextTime.Before(now) || jobPlan.NextTime.Equal(now) {
			// 尝试执行任务
			scheduler.TryStartJob(jobPlan)
			// 更新下次执行时间
			jobPlan.NextTime = jobPlan.Expr.Next(now)
		}

		// 统计最近一个要过期的任务事件
		if nearTime == nil || jobPlan.NextTime.Before(*nearTime) {
			nearTime = &jobPlan.NextTime
		}
	}

	// 下次调度时间：最近要执行的任务事件-当前时间
	scheduleAfter := (*nearTime).Sub(now)
	return scheduleAfter
}

// handlerJobResult 处理任务结果
func (scheduler *Scheduler) handlerJobResult(result *common.JobExecuteResult) {
	// 删除执行状态
	delete(scheduler.jobExecutingTable, result.ExecuteInfo.Job.Name)

	// 生成执行日志
	if result.Err != common.ERR_LOCK_ALREADY_REQUIRED {
		jobLog := &common.JobLog{
			JobName:      result.ExecuteInfo.Job.Name,
			Command:      result.ExecuteInfo.Job.Command,
			Output:       string(result.Output),
			PlanTime:     result.ExecuteInfo.PlanTime.UnixMilli(),
			ScheduleTime: result.ExecuteInfo.RealTime.UnixMilli(),
			StartTime:    result.StartTime.UnixMilli(),
			EndTime:      result.EndTime.UnixMilli(),
		}
		if result.Err != nil {
			jobLog.Err = result.Err.Error()
		} else {
			jobLog.Err = ""
		}

		// 追加日志到mongodb
		G_logSink.AppendLog(jobLog)
	}
}

// scheduleLoop 调度协程
func (scheduler *Scheduler) scheduleLoop() {
	var (
		jobEvent      *common.JobEvent
		scheduleAfter time.Duration
		scheduleTimer *time.Timer
		jobResult     *common.JobExecuteResult
	)

	// 初始化一次
	scheduleAfter = scheduler.TrySchedule()

	// 调度延迟定时器
	scheduleTimer = time.NewTimer(scheduleAfter)

	// 定时任务
	for {
		select {
		case jobEvent = <-scheduler.jobEventChan: //监听任务事件变化
			//对我们维护的任务列表做增删改查
			scheduler.handleJobEvent(jobEvent)
		case <-scheduleTimer.C: // 最近的任务到期了
		case jobResult = <-scheduler.jobResultChan: // 监听执行结果
			scheduler.handlerJobResult(jobResult)
		}
		// 调度一次任务
		scheduleAfter = scheduler.TrySchedule()
		// 重置调度间隔
		scheduleTimer.Reset(scheduleAfter)
	}
}

// PushJobEvent 推送任务变化事件
func (scheduler *Scheduler) PushJobEvent(jobEvent *common.JobEvent) {
	scheduler.jobEventChan <- jobEvent
}

// InitScheduler 初始化调度器
func InitScheduler() error {
	G_scheduler = &Scheduler{
		jobEventChan:      make(chan *common.JobEvent, 1000),
		jobPlanTable:      make(map[string]*common.JobSchedulePlan),
		jobExecutingTable: make(map[string]*common.JobExecuteInfo),
		jobResultChan:     make(chan *common.JobExecuteResult, 1000),
	}

	// 启动调度协程
	go G_scheduler.scheduleLoop()
	return nil
}

// PushJobResult 回传任务执行结果
func (scheduler *Scheduler) PushJobResult(result *common.JobExecuteResult) {
	scheduler.jobResultChan <- result
}
