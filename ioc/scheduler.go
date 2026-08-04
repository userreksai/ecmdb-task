package ioc

import (
	"github.com/userreksai/ecmdb-task/internal/service/acquirer"
	"github.com/userreksai/ecmdb-task/internal/service/picker"
	"github.com/userreksai/ecmdb-task/internal/service/runner"
	"github.com/userreksai/ecmdb-task/internal/service/scheduler"
	"github.com/userreksai/ecmdb-task/internal/service/task"
	"github.com/google/uuid"
	"github.com/spf13/viper"
)

func InitNodeID() string {
	return uuid.New().String()
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
