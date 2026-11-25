package ioc

import (
	"time"

	"github.com/ecodeclub/mq-api"
	ecmdbrunnerv1 "github.com/userreksai/ecmdb-task/api/proto/gen/ecmdb/runner/v1"
	executorv1 "github.com/userreksai/ecmdb-task/api/proto/gen/etask/executor/v1"
	grpcapi "github.com/userreksai/ecmdb-task/internal/grpc"
	"github.com/userreksai/ecmdb-task/internal/service/scheduler"
	"github.com/userreksai/ecmdb-task/internal/service/task"
	"github.com/userreksai/ecmdb-task/internal/workflow"
	"github.com/userreksai/ecmdb-task/pkg/grpc/pool"
	"google.golang.org/grpc"
	"gorm.io/gorm"
)

// InitWorkflowDAO 初始化 eflow 工作流执行请求表并返回存储。
func InitWorkflowDAO(db *gorm.DB) workflow.DAO {
	if err := workflow.InitTables(db); err != nil {
		panic(err)
	}
	return workflow.NewDAO(db)
}

// InitECMDBRunnerCatalog ECMDB 执行单元目录（ecmdb.runner.v1）。
func InitECMDBRunnerCatalog(cc grpc.ClientConnInterface) workflow.RunnerCatalog {
	return workflow.NewRunnerCatalog(ecmdbrunnerv1.NewRunnerServiceClient(cc))
}

// InitWorkflowService 组装工作流执行编排：复用现有任务、执行、调度与执行器中断能力。
func InitWorkflowService(dao workflow.DAO, catalog workflow.RunnerCatalog, taskSvc task.Service,
	execSvc task.ExecutionService, sch *scheduler.Scheduler,
	clients *pool.Clients[executorv1.ExecutorServiceClient], q mq.MQ, logSvc task.LogService) workflow.Service {
	return workflow.NewService(dao, catalog, taskSvc, execSvc, sch, workflow.NewInterrupter(clients, execSvc),
		workflow.NewAgentPublisher(q), logSvc)
}

// InitWorkflowKafkaResultConsumer Kafka Agent 回传结果的消费者（独立消费组，只处理工作流执行）。
func InitWorkflowKafkaResultConsumer(q mq.MQ, svc workflow.Service) *workflow.KafkaResultConsumer {
	return workflow.NewKafkaResultConsumer(q, svc)
}

// InitWorkflowKafkaExpireJob 超时仍未收到结果的 Kafka 执行巡检。
func InitWorkflowKafkaExpireJob(svc workflow.Service) *workflow.KafkaExpireJob {
	return workflow.NewKafkaExpireJob(svc, 30*time.Second)
}

// InitWorkflowBridge 工作流执行终态 → eflow 的事件桥接（独立消费组）。
func InitWorkflowBridge(q mq.MQ, svc workflow.Service) *workflow.Bridge {
	bridge, err := workflow.NewBridge(q, svc)
	if err != nil {
		panic(err)
	}
	return bridge
}

// InitWorkflowExecutionServer 在原 TaskExecutionService（AgentServer）上补充 GetTaskExecution。
func InitWorkflowExecutionServer(agent *grpcapi.AgentServer, execSvc task.ExecutionService,
	svc workflow.Service) *workflow.ExecutionServer {
	return workflow.NewExecutionServer(agent, execSvc, svc)
}
