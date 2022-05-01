package master

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Pangjiping/crontab/common"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// 任务管理器
type JobMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	// 单例
	G_jobMgr *JobMgr
)

// InitJobMgr 初始化管理器
func InitJobMgr() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
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

	// 赋值单例
	G_jobMgr = &JobMgr{
		kv:     kv,
		client: client,
		lease:  lease,
	}

	return
}

// SaveJob 保存任务到etcd
// /cron/jobs/jobName
func (jobMgr *JobMgr) SaveJob(job *common.Job) (*common.Job, error) {
	var (
		jobKey      string
		jobValue    []byte
		putResponse *clientv3.PutResponse
		err         error
	)

	// 定义jobkey
	jobKey = common.JOB_SAVE_DIR + job.Name

	// 定义value job结构体的json序列化
	jobValue, err = json.Marshal(job)
	if err != nil {
		return nil, err
	}

	// 保存到etcd
	// 并返回保存/修改这个任务之前的指令
	putResponse, err = jobMgr.kv.Put(context.TODO(),
		jobKey,
		string(jobValue),
		clientv3.WithPrevKV())
	if err != nil {
		return nil, err
	}

	oldJob := common.Job{}

	// 如果是更新，返回旧值
	if putResponse.PrevKv != nil {
		// 对旧值做反序列化
		err := json.Unmarshal(putResponse.PrevKv.Value, &oldJob)
		if err != nil {
			err = nil
			return nil, err
		}
	}

	return &oldJob, nil
}

// DeleteJob 从etcd中删除任务
func (jobMgr *JobMgr) DeleteJob(name string) (*common.Job, error) {
	var (
		jobKey         string
		deleteResponse *clientv3.DeleteResponse
		delJob         common.Job
		err            error
	)

	// 获取要删除任务的key
	jobKey = common.JOB_SAVE_DIR + name

	// 从etcd中删除这个任务
	// 并返回这个要删除的这个key-value
	deleteResponse, err = jobMgr.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV())
	if err != nil {
		return nil, err
	}

	delJob = common.Job{}
	// 返回被删除的kv信息
	if len(deleteResponse.PrevKvs) != 0 {
		err = json.Unmarshal(deleteResponse.PrevKvs[0].Value, &delJob)
		if err != nil {
			return nil, nil
		}
		return &delJob, nil
	}
	return nil, nil
}

// ListJobs 列举任务
// 获取/cron/jobs/目录下所有任务
func (jobMgr *JobMgr) ListJobs() ([]*common.Job, error) {
	var (
		dirKey      string
		getResponse *clientv3.GetResponse
		jobList     []*common.Job
		err         error
	)

	// 获取目录key
	dirKey = common.JOB_SAVE_DIR

	// 获取/cron/jobs/目录下所有任务信息
	getResponse, err = jobMgr.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	if len(getResponse.Kvs) == 0 {
		return nil, nil
	}

	jobList = make([]*common.Job, 0)
	// 遍历任务列表，反序列化
	for _, kvPair := range getResponse.Kvs {
		job := &common.Job{}
		json.Unmarshal(kvPair.Value, job)
		jobList = append(jobList, job)
	}
	return jobList, nil
}

// KillJob 杀死任务
// 杀死任务就是将任务写入到 /cron/killer/
func (jobMgr *JobMgr) KillJob(name string) error {
	var (
		killKey       string
		grantResponse *clientv3.LeaseGrantResponse
		leaseID       clientv3.LeaseID
		err           error
	)
	killKey = common.JOB_KILL_DIR + name

	// 让work监听到put操作即可，创建一个租约让其过期
	grantResponse, err = jobMgr.lease.Grant(context.TODO(), common.KILL_TTL) //1s租约
	if err != nil {
		return err
	}

	leaseID = grantResponse.ID

	// 设置killer标记
	_, err = jobMgr.kv.Put(context.TODO(), killKey, "", clientv3.WithLease(leaseID))
	if err != nil {
		return err
	}
	return nil
}
