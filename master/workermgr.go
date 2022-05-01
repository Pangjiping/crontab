package master

import (
	"context"
	"time"

	"github.com/Pangjiping/crontab/common"
	"go.etcd.io/etcd/api/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type WorkerMgr struct {
	client *clientv3.Client
	kv     clientv3.KV
	lease  clientv3.Lease
}

var (
	G_workerMgr *WorkerMgr
)

// ListWorkers 获取在线worker列表
// 工作的worker节点都会注册到/cron/workers/目录
func (workerMgr *WorkerMgr) ListWorkers() ([]string, error) {
	var (
		err         error
		workerArr   []string
		getResponse *clientv3.GetResponse
		kv          *mvccpb.KeyValue
		workerIP    string
	)
	workerArr = make([]string, 0)

	// 在etcd中取得/cron/workers/目录下所有节点（健康节点）
	getResponse, err = workerMgr.kv.Get(context.TODO(), common.JOB_WORKER_DIR, clientv3.WithPrefix())
	if err != nil {
		return workerArr, err
	}

	// 解析每个节点的ip
	for _, kv = range getResponse.Kvs {
		workerIP = common.ExtractWorkerIP(string(kv.Key))
		workerArr = append(workerArr, workerIP)
	}
	return workerArr, err
}

// InitWorkerMgr 初始化workerMgr
func InitWorkerMgr() error {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv     clientv3.KV
		lease  clientv3.Lease
		err    error
	)

	// etcd配置
	config = clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}

	// 新建etcd客户端
	client, err = clientv3.New(config)
	if err != nil {
		return err
	}

	// 得到kv和lease子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	// 赋值单例
	G_workerMgr = &WorkerMgr{
		client: client,
		kv:     kv,
		lease:  lease,
	}
	return err
}
