package ioc

import (
	"github.com/userreksai/ecmdb-task/internal/compensator"
	"github.com/userreksai/ecmdb-task/internal/workflow"
)

func InitTasks(
	t1 *compensator.RetryCompensator,
	t2 *compensator.RescheduleCompensator,
	t3 *compensator.InterruptCompensator,
	t4 *CompleteConsumer,
	t5 *workflow.Bridge, // eflow 工作流执行终态回传
) []Task {
	return []Task{
		t1,
		t2,
		t3,
		t4,
		t5,
	}
}
