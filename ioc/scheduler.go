package ioc

import (
	"sync"

	"github.com/userreksai/ecmdb-task/internal/service/acquirer"
	"github.com/userreksai/ecmdb-task/internal/service/picker"
	"github.com/userreksai/ecmdb-task/internal/service/runner"
	"github.com/userreksai/ecmdb-task/internal/service/scheduler"
	"github.com/userreksai/ecmdb-task/internal/service/task"
	"github.com/google/uuid"
	"github.com/spf13/viper"
)

var (
	nodeIDOnce sync.Once
	nodeID     string
)

// InitNodeID 返回本进程的调度节点 ID。
//
// 进程内所有模块共享同一个 ID：scheduler 模式下调度循环与 gRPC 服务端分别构造了 Scheduler 实例，
// eflow 提交的工作流执行由 gRPC 服务端立即抢占，必须与调度循环使用同一 ID，
// 才能被调度循环的续约（Renew）覆盖，避免长任务被误判为僵尸任务而重复执行。
func InitNodeID() string {
	nodeIDOnce.Do(func() { nodeID = uuid.New().String() })
	return nodeID
}

func InitScheduler(
	nodeID string,
	runner runner.Runner,
	taskSvc task.Service,
	execSvc task.ExecutionService,
	acquirer acquirer.TaskAcquirer,
	nodePicker picker.ExecutorNodePicker,
	modeResolver picker.IExecModeResolver,
) *scheduler.Scheduler {
	var cfg scheduler.Config
	err := viper.UnmarshalKey("scheduler", &cfg)
	if err != nil {
		panic(err)
	}

	return scheduler.NewScheduler(
		nodeID,
		runner,
		taskSvc,
		execSvc,
		acquirer,
		cfg,
		nodePicker,
		modeResolver,
	)
}
