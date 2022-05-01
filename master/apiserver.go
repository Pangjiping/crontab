package master

import (
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/Pangjiping/crontab/common"
)

// 任务的http接口
type ApiServer struct {
	httpServer *http.Server
}

var (
	// 单例对象
	G_apiServer *ApiServer
)

// handleJobSave 保存任务接口
// post job={"name":"job1","commond":"echo hello" ...}
func handleJobSave(w http.ResponseWriter, r *http.Request) {

	// 解析post参数
	err := r.ParseForm()
	if err != nil {
		bytes, _ := common.BuildResponse(-1, err.Error(), nil)
		w.WriteHeader(http.StatusBadRequest)
		w.Write(bytes)
		return
	}

	// 取表单中的job字段
	postJob := r.PostForm.Get("job")
	job := common.Job{}

	// 反序列化job
	err = json.Unmarshal([]byte(postJob), &job)
	if err != nil {
		bytes, _ := common.BuildResponse(-1, err.Error(), nil)
		w.WriteHeader(http.StatusBadRequest)
		w.Write(bytes)
		return
	}

	// 任务保存在etcd中
	oldJob, err := G_jobMgr.SaveJob(&job)
	if err != nil {
		bytes, _ := common.BuildResponse(-1, err.Error(), nil)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(bytes)
		return
	}

	// 正常应答
	bytes, _ := common.BuildResponse(0, "success", oldJob)
	w.WriteHeader(http.StatusOK)
	w.Write(bytes)
}

// handleJobDelete 任务删除接口
// POST name=job1
func handleJobDelete(w http.ResponseWriter, r *http.Request) {

	// 解析post参数
	err := r.ParseForm()
	if err != nil {
		bytes, _ := common.BuildResponse(-1, err.Error(), nil)
		w.WriteHeader(http.StatusBadRequest)
		w.Write(bytes)
		return
	}

	// 要删除的任务名
	delJobName := r.PostForm.Get("name")

	// 在etcd中删除任务
	oldJob, err := G_jobMgr.DeleteJob(delJobName)
	if err != nil {
		bytes, _ := common.BuildResponse(-1, err.Error(), nil)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(bytes)
		return
	}

	// 正常应答
	bytes, _ := common.BuildResponse(0, "success", oldJob)
	w.WriteHeader(http.StatusOK)
	w.Write(bytes)
}

// handleJobList 列举所有crontab任务
func handleJobList(w http.ResponseWriter, r *http.Request) {

	// 从etcd中获取任务列表
	jobList, err := G_jobMgr.ListJobs()
	if err != nil {
		bytes, _ := common.BuildResponse(-1, err.Error(), nil)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(bytes)
		return
	}

	// 正常应答
	bytes, _ := common.BuildResponse(0, "success", jobList)
	w.WriteHeader(http.StatusOK)
	w.Write(bytes)
}

// handleJobKill 强制杀死某个任务
func handleJobKill(w http.ResponseWriter, r *http.Request) {

	// 解析表单参数
	err := r.ParseForm()
	if err != nil {
		bytes, _ := common.BuildResponse(-1, err.Error(), nil)
		w.WriteHeader(http.StatusBadRequest)
		w.Write(bytes)
		return
	}

	// 获取要kill的任务名
	killName := r.PostForm.Get("name")

	// 写入etcd kill目录
	err = G_jobMgr.KillJob(killName)
	if err != nil {
		bytes, _ := common.BuildResponse(-1, err.Error(), nil)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(bytes)
		return
	}

	// 正常应答
	bytes, _ := common.BuildResponse(0, "success", nil)
	w.WriteHeader(http.StatusOK)
	w.Write(bytes)
}

// handleJobLog 查询任务日志
func handleJobLog(w http.ResponseWriter, r *http.Request) {

	// 解析get参数
	err := r.ParseForm()
	if err != nil {
		bytes, _ := common.BuildResponse(-1, err.Error(), nil)
		w.WriteHeader(http.StatusBadRequest)
		w.Write(bytes)
		return
	}

	// 获取请求参数 /job/log?name=job10&skip=0&limit=10
	name := r.Form.Get("name")
	skipParam := r.Form.Get("skip")
	limitParam := r.Form.Get("limit")

	// 字符串转int
	skip, err := strconv.Atoi(skipParam)
	if err != nil {
		skip = 0
	}
	limit, err := strconv.Atoi(limitParam)
	if err != nil {
		limit = 20
	}

	// 从mongodb下载日志
	logArr, err := G_logMgr.ListLog(name, int64(skip), int64(limit))
	if err != nil {
		bytes, _ := common.BuildResponse(-1, err.Error(), nil)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(bytes)
		return
	}

	// 正常应答
	bytes, _ := common.BuildResponse(0, "success", logArr)
	w.WriteHeader(http.StatusOK)
	w.Write(bytes)

}

// handlerWorkerList 获取健康worker节点列表
func handlerWorkerList(w http.ResponseWriter, r *http.Request) {
	var (
		workerArr []string
		err       error
		bytes     []byte
	)

	// 从etcd中获取健康节点
	workerArr, err = G_workerMgr.ListWorkers()
	if err != nil {
		bytes, _ = common.BuildResponse(-1, err.Error(), nil)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write(bytes)
		return
	}

	// 正常应答
	bytes, _ = common.BuildResponse(0, "success", workerArr)
	w.WriteHeader(http.StatusOK)
	w.Write(bytes)

}

// InitApiServer 初始化http服务
func InitApiServer() (err error) {
	var (
		mux        *http.ServeMux
		listener   net.Listener
		httpServer *http.Server
	)

	// 配置路由
	mux = http.NewServeMux()
	mux.HandleFunc("/job/save", handleJobSave)
	mux.HandleFunc("/job/delete", handleJobDelete)
	mux.HandleFunc("/job/list", handleJobList)
	mux.HandleFunc("/job/kill", handleJobKill)
	mux.HandleFunc("/job/log", handleJobLog)
	mux.HandleFunc("/worker/list", handlerWorkerList)

	// 静态文件目录
	staticDir := http.Dir("./webroot")
	// 静态文件的http回调
	staticHandler := http.FileServer(staticDir)
	// 注册静态文件到路由
	mux.Handle("/", http.StripPrefix("/", staticHandler))

	// 启动tcp监听
	listener, err = net.Listen("tcp", ":"+strconv.Itoa(G_config.ApiPort))
	if err != nil {
		return
	}

	// 创建http服务
	httpServer = &http.Server{
		ReadTimeout:  time.Duration(G_config.ApiReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(G_config.ApiWriteTimeout) * time.Millisecond,
		Handler:      mux,
	}

	G_apiServer = &ApiServer{
		httpServer: httpServer,
	}

	// 启动服务端
	go httpServer.Serve(listener)

	return
}
