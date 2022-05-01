package worker

import (
	"encoding/json"
	"io/ioutil"
)

// worker配置
type Config struct {
	EtcdEndpoints         []string `json:"etcdEndpoints"`         // etcd集群信息
	EtcdDialTimeout       int      `json:"etcdDialTimeout"`       // etcd连接超时
	MongodbUri            string   `json:"mongodbUri"`            // mongodb地址
	MongodbConnectTimeout int      `json:"mongodbConnectTimeout"` // mongodb连接超时
	JobLogBatchSize       int      `json:"jobLogBatchSize"`       // 打包上传log的batch大小
	JobLogCommitTimeout   int      `json:"jobLogCommitTimeout"`   // 日志提交超时时间
}

var (
	// 单例对象
	G_config *Config
)

// 初始化master配置
func InitConfig(filename string) (err error) {
	var (
		content []byte
		conf    Config
	)

	// 读取配置文件
	content, err = ioutil.ReadFile(filename)
	if err != nil {
		return
	}

	// json反序列化
	err = json.Unmarshal(content, &conf)
	if err != nil {
		return
	}
	G_config = &conf
	return
}
