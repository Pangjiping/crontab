package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/Pangjiping/crontab/common"
	"go.etcd.io/etcd/api/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// 任务管理器
type JobMgr struct {
	client  *clientv3.Client // etcd客户端
	kv      clientv3.KV      // etcd KV
	lease   clientv3.Lease   // etcd租约
	watcher clientv3.Watcher // 主要用来监听killer
}

var (
	// 单例
	G_jobMgr *JobMgr
)

// watchJobs 监听任务变化
func (jobMgr *JobMgr) watchJobs() error {
	var (
		watchStartRevision int64
		watchChan          clientv3.WatchChan
		watchResp          clientv3.WatchResponse
		watchEvent         *clientv3.Event
		jobEvent           *common.JobEvent
	)

	// get /cron/jobs/下的所有任务，并且获知当前集群的revision
	getResponse, err := jobMgr.kv.Get(context.TODO(), common.JOB_SAVE_DIR, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	// 当前有哪些任务
	for _, kvPair := range getResponse.Kvs {
		job, err := common.UnpackJob(kvPair.Value)
		if err == nil {
			jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)
			//todo:把这个job同步给调度协程

			G_scheduler.PushJobEvent(jobEvent)
		}
	}

	// 从该revision向后监听变化事件
	go func() {
		// 从get的后续事件开始监听
		watchStartRevision = getResponse.Header.Revision + 1
		// 启动监听
		watchChan = jobMgr.watcher.Watch(context.TODO(), common.JOB_SAVE_DIR,
			clientv3.WithRev(watchStartRevision), clientv3.WithPrefix())
		// 处理监听事件
		for watchResp = range watchChan {
			for _, watchEvent = range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: //任务保存事件
					job, err := common.UnpackJob(watchEvent.Kv.Value)
					if err != nil {
						continue
					}
					// 构建一个更新event事件
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_SAVE, job)

				case mvccpb.DELETE: //任务删除事件
					jobName := common.ExtractJobName(string(watchEvent.Kv.Key))
					job := &common.Job{Name: jobName}
					// 构造一个删除event
					jobEvent = common.BuildJobEvent(common.JOB_EVENT_DELETE, job)
				}

				// h推送删除给scheduler
				G_scheduler.PushJobEvent(jobEvent)
			}
		}
	}()
	return nil
}

// watchKiller 监听强杀任务通知
func (jobMgr *JobMgr) watchKiller() {
	// 监听/cron/killer/目录
	go func() {
		fmt.Println("进入监听协程")
		watchChan := jobMgr.watcher.Watch(context.TODO(), common.JOB_KILL_DIR, clientv3.WithPrefix())
		// 处理监听任务
		for watchResp := range watchChan {
			for _, watchEvent := range watchResp.Events {
				switch watchEvent.Type {
				case mvccpb.PUT: //杀死某个任务的事件
					jobName := common.ExtractKillerName(string(watchEvent.Kv.Key))
					job := &common.Job{
						Name: jobName,
					}
					jobEvent := &common.JobEvent{
						EventType: common.JOB_EVENT_KILL,
						Job:       job,
					}
					// 事件推给scheduler
					G_scheduler.PushJobEvent(jobEvent)
					fmt.Println("强杀任务推送给scheduler")
				case mvccpb.DELETE: // killer标记过期，被自动删除
				}
			}
		}
	}()
}

// 初始化管理器
func InitJobMgr() (err error) {
	var (
		config  clientv3.Config
		client  *clientv3.Client
		kv      clientv3.KV
		lease   clientv3.Lease
		watcher clientv3.Watcher
	)

	//初始化配置
	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}

	// 建立连接
	client, err = clientv3.New(config)
	if err != nil {
		return
	}

	// 得到kv和lease
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)
	watcher = clientv3.NewWatcher(client)

	// 赋值单例
	G_jobMgr = &JobMgr{
		kv:      kv,
		client:  client,
		lease:   lease,
		watcher: watcher,
	}

	// 启动任务监听
	G_jobMgr.watchJobs()

	// 启动监听killer
	G_jobMgr.watchKiller()

	return
}

// 创建任务执行锁
func (jobMgr *JobMgr) CreateJobLock(name string) *JobLock {
	// 返回一把锁
	return InitJobLock(name, jobMgr.kv, jobMgr.lease)
}
