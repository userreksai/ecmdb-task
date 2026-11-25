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
	t6 *workflow.KafkaResultConsumer, // Kafka Agent 执行结果（工作流执行）
	t7 *workflow.KafkaExpireJob, // Kafka Agent 执行超时巡检
) []Task {
	return []Task{
		t1,
		t2,
		t3,
		t4,
		t5,
		t6,
		t7,
	}
}
