package master

import (
	"context"
	"time"

	"github.com/Pangjiping/crontab/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// mongodb日志管理
type LogMgr struct {
	client        *mongo.Client
	logCollection *mongo.Collection
}

var (
	G_logMgr *LogMgr
)

// InitLogMgr 初始化日志管理
func InitLogMgr() error {
	client, err := mongo.Connect(context.TODO(),
		options.Client().ApplyURI(G_config.MongodbUri),
		options.Client().SetConnectTimeout(time.Duration(G_config.MongodbConnectTimeout)*time.Millisecond))
	if err != nil {
		return err
	}

	G_logMgr = &LogMgr{
		client:        client,
		logCollection: client.Database("cron").Collection("log"),
	}
	return nil
}

// ListLog 列举某个任务的日志
func (logMgr *LogMgr) ListLog(name string, skip int64, limit int64) ([]*common.JobLog, error) {
	var (
		filter  *common.JobLogFilter
		logSort *common.SortLogByStartTime
		cursor  *mongo.Cursor
		logArr  []*common.JobLog
		err     error
	)

	// 构造一个过滤条件
	filter = &common.JobLogFilter{JobName: name}

	// 按照任务开始时间倒排
	logSort = &common.SortLogByStartTime{SortOrder: -1}

	// 查询
	cursor, err = logMgr.logCollection.Find(context.TODO(), filter, &options.FindOptions{
		Sort:  logSort,
		Skip:  &skip,
		Limit: &limit,
	})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.TODO())

	logArr = make([]*common.JobLog, 0)

	// 遍历游标取得日志
	for cursor.Next(context.TODO()) {
		jobLog := &common.JobLog{}
		// 反序列化bson
		err = cursor.Decode(jobLog)
		if err != nil {
			continue // 不合法日志忽略
		}
		logArr = append(logArr, jobLog)
	}
	return logArr, nil

}
