package ioc

import (
	"github.com/google/wire"
	agentSvc "github.com/userreksai/ecmdb-task/internal/agent"
	"github.com/userreksai/ecmdb-task/internal/agent/web"
	"github.com/userreksai/ecmdb-task/internal/grpc"
	"github.com/userreksai/ecmdb-task/internal/repository"
	"github.com/userreksai/ecmdb-task/internal/repository/dao"
	categorySvc "github.com/userreksai/ecmdb-task/internal/service/category"
	taskSvc "github.com/userreksai/ecmdb-task/internal/service/task"
	"github.com/userreksai/ecmdb-task/internal/web/executor"
	"github.com/userreksai/ecmdb-task/internal/web/manager"
)

var (
	BaseSet = wire.NewSet(
		InitEtcdClient,
		InitMQ,
		InitRegistry,
	)

	WebSetup = wire.NewSet(
		InitECMDBGrpcClient,
		InitEndpointServiceClient,
		InitPolicySDK,
		InitPermSyncer,
		InitProviders,
		InitListener,
		InitGinMiddlewares,
		InitGinWebServer,
	)

	TaskSet = wire.NewSet(
		InitDB,
		dao.NewGORMTaskDAO,
		dao.NewGORMTaskCategoryDAO,
		repository.NewTaskRepository,
		repository.NewTaskCategoryRepository,
		repository.NewTaskExecutionLogRepository,
		taskSvc.NewService,
		categorySvc.NewService,
		taskSvc.NewLogService,
		manager.NewHandler,
	)

	ExecutorSet = wire.NewSet(
		executor.NewHandler,
	)

	TaskExecutionSet = wire.NewSet(
		dao.NewGORMTaskExecutionDAO,
		dao.NewGORMTaskExecutionLogDAO,
		repository.NewTaskExecutionRepository,
		taskSvc.NewExecutionService,
	)

	SchedulerSet = wire.NewSet(
		InitNodeID,
		InitScheduler,
		InitMySQLTaskAcquirer,
		InitExecutorNodePicker,
		InitExecModeResolver,
	)

	AgentSet = wire.NewSet(
		agentSvc.InitModule,
		wire.FieldsOf(new(*agentSvc.Module), "Hdl"),
	)

	// AgentWebSet 专门用于 Scheduler 等只需要查看 Agent 状态而不运行 Agent 的场景
	AgentWebSet = wire.NewSet(
		web.NewHandler,
	)

	CompensatorSet = wire.NewSet(
		InitRetryCompensator,
		InitRescheduleCompensator,
		InitInterruptCompensator,
	)

	ProducerSet = wire.NewSet(
		InitCompleteProducer,
	)

	GrpcSet = wire.NewSet(
		InitExecutorServiceGRPCClients,
	)

	ConsumerSet = wire.NewSet(
		InitCompleteEventConsumer,
	)

	// AppSet 包含 Scheduler 模式的核心 Provider
	AppSet = wire.NewSet(
		grpc.NewReporterServer,
		grpc.NewTaskServer,
		grpc.NewAgentServer,
		InitTasks,
		InitSchedulerNodeGRPCServer,
	)
)
