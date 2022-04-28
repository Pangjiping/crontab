package master

import (
	"context"
	"encoding/json"
	"github.com/Pangjiping/crontab/common"
	clientv3 "go.etcd.io/etcd/client/v3"
	"time"
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

// 初始化管理器
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

// 保存任务到etcd
func (jobMgr *JobMgr) SaveJob(job *common.Job) (*common.Job, error) {
	//把任务保存在/cron/jobs/任务名下 -> json

	// 定义jobkey
	jobKey := common.JOB_SAVE_DIR + job.Name

	// 定义value
	jobValue, err := json.Marshal(job)
	if err != nil {
		return nil, err
	}

	// 保存到etcd
	putResponse, err := jobMgr.kv.Put(context.TODO(),
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

// 从etcd中删除任务
func (jobMgr *JobMgr) DeleteJob(name string) (*common.Job, error) {
	jobKey := common.JOB_SAVE_DIR + name
	deleteResponse, err := jobMgr.kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV())
	if err != nil {
		return nil, err
	}

	delJob := common.Job{}
	// 返回被删除的kv信息
	if len(deleteResponse.PrevKvs) != 0 {
		err := json.Unmarshal(deleteResponse.PrevKvs[0].Value, &delJob)
		if err != nil {
			return nil, nil
		}
		return &delJob, nil
	}
	return nil, nil
}

// 列举任务
func (jobMgr *JobMgr) ListJobs() ([]*common.Job, error) {
	dirKey := common.JOB_SAVE_DIR

	// 获取目录下所有任务信息
	getResponse, err := jobMgr.kv.Get(context.TODO(), dirKey, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	if len(getResponse.Kvs) == 0 {
		return nil, nil
	}

	jobList := make([]*common.Job, 0)
	// 遍历任务列表，反序列化
	for _, kvPair := range getResponse.Kvs {
		job := &common.Job{}
		json.Unmarshal(kvPair.Value, job)
		jobList = append(jobList, job)
	}
	return jobList, nil
}

// 杀死任务
func (jobMgr *JobMgr) KillJob(name string) error {
	// 写入：/cron/killer/任务名

	killKey := common.JOB_KILL_DIR + name

	// 让work监听到put操作即可，创建一个租约让其过期
	grantResponse, err := jobMgr.lease.Grant(context.TODO(), common.KILL_TTL) //1s租约
	if err != nil {
		return err
	}

	leaseID := grantResponse.ID

	// 设置killer标记
	_, err = jobMgr.kv.Put(context.TODO(), killKey, "", clientv3.WithLease(leaseID))
	if err != nil {
		return err
	}
	return nil
}
