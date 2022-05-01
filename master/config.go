package master

import (
	"encoding/json"
	"io/ioutil"
)

// master配置
type Config struct {
	ApiPort               int      `json:"apiPort"`
	ApiReadTimeout        int      `json:"apiReadTimeout"`
	ApiWriteTimeout       int      `json:"apiWriteTimeout"`
	EtcdEndpoints         []string `json:"etcdEndpoints"`
	EtcdDialTimeout       int      `json:"etcdDialTimeout"`
	Webroot               string   `json:"webroot"`
	MongodbUri            string   `json:"mongodbUri"`
	MongodbConnectTimeout int      `json:"mongodbConnectTimeout"`
}

var (
	// 单例对象
	G_config *Config
)

// InitConfig 初始化master配置
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
