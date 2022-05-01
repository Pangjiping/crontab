package worker

import (
	"context"
	"net"
	"time"

	"github.com/Pangjiping/crontab/common"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Register 服务注册
// /cron/workers/
type Register struct {
	client  *clientv3.Client // etcd客户端
	kv      clientv3.KV      // etcd kv
	lease   clientv3.Lease   // etcd lease租约
	localIP string           // 工作节点的ipv4地址
}

var G_register *Register

// getLocalIP 得到工作节点的ip地址
func getLocalIP() (string, error) {
	var (
		addrs []net.Addr // addr slice
		addr  net.Addr   // net.addr
		err   error      // err
		ipv4  string     // required ipv4 address
	)

	// 获取本地网络信息
	addrs, err = net.InterfaceAddrs()
	if err != nil {
		return ipv4, nil
	}

	for _, addr = range addrs {
		// 确保这是一个ipv4或者ipv6地址
		ipNet, isIPNet := addr.(*net.IPNet)
		// 如果是ip地址，并且不是环回地址
		if isIPNet && !ipNet.IP.IsLoopback() {
			// 获取第一个ipv4地址
			if ipNet.IP.To4() != nil {
				ipv4 := ipNet.String()
				return ipv4, nil
			}
		}
	}
	err = common.ERR_NO_LOCAL_IP_FOUND
	return "", err
}

// keepOnline 向etcd注册这个节点
func (register *Register) keepOnline() {
	var (
		registerKey        string                                  // registerKey /cron/workers/+ipv4
		leaseGrantResponse *clientv3.LeaseGrantResponse            // etcd LeaseGrantResponse
		err                error                                   // error
		keepAliveRespChan  <-chan *clientv3.LeaseKeepAliveResponse // etcd LeaseKeepAliveResponse channel
		keepAliveResp      *clientv3.LeaseKeepAliveResponse        // etcd LeaseKeepAliveResponse
		cancelCtx          context.Context                         // context to bind cancelFunc
		cancelFunc         context.CancelFunc                      // cancelFunc to cancel auto lease renewal
	)
	for {
		cancelFunc = nil

		// 注册的key /cron/workers/+ipv4
		registerKey = common.JOB_WORKER_DIR + register.localIP

		// 创建租约
		// 所有的操作失败之后都会重试，下同
		leaseGrantResponse, err = register.lease.Grant(context.TODO(), 10)
		if err != nil {
			goto RETRY
		}

		// 自动续租
		keepAliveRespChan, err = register.lease.KeepAlive(context.TODO(), leaseGrantResponse.ID)
		if err != nil {
			goto RETRY
		}

		// 用于取消自动续租的context
		cancelCtx, cancelFunc = context.WithCancel(context.TODO())

		// 注册到etcd
		_, err = register.kv.Put(cancelCtx, registerKey, register.localIP,
			clientv3.WithLease(leaseGrantResponse.ID))
		if err != nil {
			goto RETRY
		}

		// 处理续租应答
		for {
			select {
			case keepAliveResp = <-keepAliveRespChan:
				if keepAliveResp == nil { // 租续失败
					goto RETRY
				}
			}
		}

	RETRY:
		time.Sleep(time.Second) // 休眠一秒重试
		if cancelFunc != nil {  // 取消自动续租
			cancelFunc()
		}
	}

}

// InitRegister 初始化服务注册
func InitRegister() error {

	// 得到本机ip
	localIP, err := getLocalIP()
	if err != nil {
		return err
	}

	// 初始化etcd配置
	config := clientv3.Config{
		Endpoints:   G_config.EtcdEndpoints,
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond,
	}

	// 建立连接
	client, err := clientv3.New(config)
	if err != nil {
		return err
	}

	// 得到kv和lease
	kv := clientv3.NewKV(client)
	lease := clientv3.NewLease(client)

	G_register = &Register{
		client:  client,
		kv:      kv,
		lease:   lease,
		localIP: localIP,
	}

	// 服务注册
	go G_register.keepOnline()

	return nil
}
