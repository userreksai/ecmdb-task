package ioc

import (
	"github.com/userreksai/ecmdb-task/internal/event"
	"github.com/userreksai/ecmdb-task/internal/service/acquirer"
	"github.com/userreksai/ecmdb-task/internal/service/invoker"
	"github.com/userreksai/ecmdb-task/internal/service/runner"
	"github.com/userreksai/ecmdb-task/internal/service/task"
)

func InitRunner(
	nodeID string,
	taskSvc task.Service,
	execSvc task.ExecutionService,
	taskAcquirer acquirer.TaskAcquirer,
	invoker invoker.Invoker,
	producer event.CompleteProducer,
) runner.Runner {
	return runner.NewNormalTaskRunner(
		nodeID,
		taskSvc,
		execSvc,
		taskAcquirer,
		invoker,
		producer,
	)
}
