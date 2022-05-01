package common

const (
	// Job save key prefix in etcd
	JOB_SAVE_DIR = "/cron/jobs/"
	// Job kill key prefix in etcd
	JOB_KILL_DIR = "/cron/killer/"
	// Distributed lock prefix in etcd
	JOB_LOCK_DIR = "/cron/lock/"
	// Worker node key prefix in etcd
	JOB_WORKER_DIR = "/cron/workers/"
	// Kill job lease ttl
	KILL_TTL = 1000
	// Job event type: save
	JOB_EVENT_SAVE = 1
	// Job event type: delete
	JOB_EVENT_DELETE = 2
	// Job event type: kill
	JOB_EVENT_KILL = 3
)
