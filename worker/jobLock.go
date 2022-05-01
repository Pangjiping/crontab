package worker

import (
	"context"

	"github.com/Pangjiping/crontab/common"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// 分布式锁
type JobLock struct {
	kv         clientv3.KV
	lease      clientv3.Lease
	jobName    string             // 任务名
	cancelFunc context.CancelFunc // 用于终止自动续租
	leaseID    clientv3.LeaseID   // 租约id
	isLocked   bool               // 是否上锁成功
}

// InitJobLock 初始化一把锁
func InitJobLock(name string, kv clientv3.KV, lease clientv3.Lease) *JobLock {
	return &JobLock{
		jobName: name,
		kv:      kv,
		lease:   lease,
	}
}

// 尝试上锁
func (jobLock *JobLock) TryLock() error {

	// 创建租约 5s
	leaseGrantResponse, err := jobLock.lease.Grant(context.TODO(), 5)
	if err != nil {
		return err
	}

	// context用于取消自动续租
	cancelCtx, cancelFunc := context.WithCancel(context.TODO())

	// 租约id
	leaseID := leaseGrantResponse.ID

	// 自动续租
	keepRespChan, err := jobLock.lease.KeepAlive(cancelCtx, leaseID)
	if err != nil {
		cancelFunc()
		jobLock.lease.Revoke(context.TODO(), leaseID)
		return err
	}

	// 处理续租应答的协程
	go func() {
		var keepResp *clientv3.LeaseKeepAliveResponse
		for {
			select {
			case keepResp = <-keepRespChan: // 自动续租的应答
				if keepResp == nil {
					goto END
				}
			}
		}
	END:
	}()

	// 创建事务 txn
	txn := jobLock.kv.Txn(context.TODO())

	// 锁路径
	lockKey := common.JOB_LOCK_DIR + jobLock.jobName

	// 事务抢锁
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseID))).
		Else(clientv3.OpGet(lockKey))

	// 提交事务
	txnResponse, err := txn.Commit()
	if err != nil {
		cancelFunc()
		jobLock.lease.Revoke(context.TODO(), leaseID)
		return err
	}

	// 成功返回，失败释放租约
	if !txnResponse.Succeeded {
		cancelFunc()
		jobLock.lease.Revoke(context.TODO(), leaseID)
		err := common.ERR_LOCK_ALREADY_REQUIRED
		return err
	}

	// 抢锁成功
	jobLock.cancelFunc = cancelFunc
	jobLock.leaseID = leaseID
	jobLock.isLocked = true
	return nil
}

// 释放锁
func (jobLock *JobLock) Unlock() {
	if jobLock.isLocked {
		jobLock.cancelFunc()
		jobLock.lease.Revoke(context.TODO(), jobLock.leaseID)
	}
}
