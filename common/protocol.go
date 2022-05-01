package common

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/gorhill/cronexpr"
)

// Job is a job infermation
type Job struct {
	Name     string `json:"name"`     // job name
	Command  string `json:"command"`  // job command --echo hello
	CronExpr string `json:"cronExpr"` // job cron expression --*/5 * * * *
}

// JobSchedulePlan describes the scheduling information of job.
type JobSchedulePlan struct {
	Job      *Job                 // job struct pointer
	Expr     *cronexpr.Expression // parsed cron expression
	NextTime time.Time            // job's next scheduling time
}

// JobExecuteInfo describes the executing infomation of job.
type JobExecuteInfo struct {
	Job        *Job               // job infermation
	PlanTime   time.Time          // scheduling time in theory
	RealTime   time.Time          // scheduling time in real
	CancelCtx  context.Context    // context binding cancelFunc to interrupt command
	CancelFunc context.CancelFunc // call cancelFunc to interrupt command
}

// Response is a http response struct
type Response struct {
	Errno int         `json:"errno"` // -1 is error, 0 is success
	Msg   string      `json:"msg"`   // error infomation or success infomation
	Data  interface{} `json:"data"`  // response data
}

// JobEvent describes a job's event type
type JobEvent struct {
	EventType int  // event type: save: 1, delete: 2, kill: 3
	Job       *Job // job infomation
}

// JobExecuteResult describes a job's executed result
type JobExecuteResult struct {
	ExecuteInfo *JobExecuteInfo // executing infomation of jobs
	Output      []byte          // command outputs
	Err         error           // command error
	StartTime   time.Time       // command start time
	EndTime     time.Time       // command end time
}

// JobLog describes a job's executed log
type JobLog struct {
	JobName      string `json:"jobName" bson:"jobName"`           // job name
	Command      string `json:"command" bson:"command"`           // command
	Err          string `json:"err" bson:"error"`                 // command error
	Output       string `json:"output" bson:"output"`             // command outputs
	PlanTime     int64  `json:"planTime" bson:"planTime"`         // job start time in theory
	ScheduleTime int64  `json:"scheduleTime" bson:"scheduleTime"` // job schedule time in real
	StartTime    int64  `json:"startTime" bson:"startTime"`       // job start time
	EndTime      int64  `json:"endTime" bson:"endTime"`           // job end time
}

// LogBatch is a batch of logs
type LogBatch struct {
	Logs []interface{}
}

// JobLogFilter is mongodb find filter by jobname
type JobLogFilter struct {
	JobName string `bson:"jobName"`
}

// SortLogByStartTime is mongodb find sort filter
type SortLogByStartTime struct {
	SortOrder int `bson:"startTime"` // startTime = -1 is reverse order
}

// BuildResponse builds a respones []byte to client
func BuildResponse(errno int, msg string, data interface{}) ([]byte, error) {
	response := Response{
		Errno: errno,
		Msg:   msg,
		Data:  data,
	}

	// serialize to json
	respJson, err := json.Marshal(response)
	if err != nil {
		return []byte{}, err
	}
	return respJson, nil
}

// UnpackJob deserializes json to job object
func UnpackJob(value []byte) (*Job, error) {
	job := &Job{}
	err := json.Unmarshal(value, job)
	if err != nil {
		return nil, err
	}
	return job, nil
}

// ExtractJobName extracts job name from etcd key
// "/cron/jobs/job10" --> "job10"
func ExtractJobName(jobKey string) string {
	return strings.TrimPrefix(jobKey, JOB_SAVE_DIR)
}

// ExtractKillerName extracts killed job name from etcd key
// "/cron/killer/job10" --> "job10"
func ExtractKillerName(jobKey string) string {
	return strings.TrimPrefix(jobKey, JOB_KILL_DIR)
}

// ExtractWorkerIP extracts worker ipv4 from etcd key
// "/cron/workers/192.168.0.1" --> "192.168.0.1"
func ExtractWorkerIP(workerKey string) string {
	return strings.TrimPrefix(workerKey, JOB_WORKER_DIR)
}

// BuildJobEvent builds JobEvent
func BuildJobEvent(eventType int, job *Job) *JobEvent {
	return &JobEvent{
		EventType: eventType,
		Job:       job,
	}
}

// BuildJobSchedulePlan builds JobSchedulePlan
func BuildJobSchedulePlan(job *Job) (*JobSchedulePlan, error) {

	// parse cron expression
	expr, err := cronexpr.Parse(job.CronExpr)
	if err != nil {
		return nil, err
	}

	jobSchedulePlan := &JobSchedulePlan{
		Job:      job,
		Expr:     expr,
		NextTime: expr.Next(time.Now()),
	}
	return jobSchedulePlan, nil
}

// BulidJobExecuteInfo builds JobExecuteInfo
func BulidJobExecuteInfo(plan *JobSchedulePlan) *JobExecuteInfo {
	jobExecuteInfo := &JobExecuteInfo{
		Job:      plan.Job,
		PlanTime: plan.NextTime,
		RealTime: time.Now(),
	}
	jobExecuteInfo.CancelCtx, jobExecuteInfo.CancelFunc = context.WithCancel(context.TODO())
	return jobExecuteInfo
}
