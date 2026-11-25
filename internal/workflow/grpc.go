package workflow

import (
	"context"
	"errors"

	ecmdbrunnerv1 "github.com/userreksai/ecmdb-task/api/proto/gen/ecmdb/runner/v1"
	executorv1 "github.com/userreksai/ecmdb-task/api/proto/gen/etask/executor/v1"
	runnerv1 "github.com/userreksai/ecmdb-task/api/proto/gen/etask/runner/v1"
	schedulerv1 "github.com/userreksai/ecmdb-task/api/proto/gen/etask/scheduler/v1"
	"github.com/userreksai/ecmdb-task/internal/domain"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SchedulerServer 实现 etask.scheduler.v1.SchedulerService。
type SchedulerServer struct {
	schedulerv1.UnimplementedSchedulerServiceServer
	svc Service
}

// NewSchedulerServer 创建调度协议服务。
func NewSchedulerServer(svc Service) *SchedulerServer { return &SchedulerServer{svc: svc} }

func (s *SchedulerServer) RunRunner(ctx context.Context, req *schedulerv1.RunRunnerRequest) (*schedulerv1.RunRunnerResponse, error) {
	res, err := s.svc.RunRunner(ctx, RunRequest{
		RequestID: req.GetRequestId(), RunnerID: req.GetRunnerId(),
		Params: req.GetParams(), Variables: req.GetVariables(),
		MaxExecutionSeconds: req.GetMaxExecutionSeconds(),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.RunRunnerResponse{ExecutionId: res.ExecutionID, Created: res.Created}, nil
}

func (s *SchedulerServer) TerminateExecution(ctx context.Context,
	req *schedulerv1.TerminateExecutionRequest) (*schedulerv1.TerminateExecutionResponse, error) {
	if err := s.svc.TerminateExecution(ctx, req.GetExecutionId(), req.GetRequestId(), req.GetReason()); err != nil {
		return nil, toStatus(err)
	}
	return &schedulerv1.TerminateExecutionResponse{Terminated: true}, nil
}

// toStatus 映射错误码：eflow 把 InvalidArgument / FailedPrecondition 视为"不可重试的拒绝"，
// 其它错误视为传输错误并用同一 request_id 幂等重投。
func toStatus(err error) error {
	switch {
	case errors.Is(err, ErrInvalidRequest):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrRejected):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ErrExecutionPending):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// RunnerServer 实现 etask.runner.v1.RunnerService（代理到 ECMDB）。
type RunnerServer struct {
	runnerv1.UnimplementedRunnerServiceServer
	catalog RunnerCatalog
}

// NewRunnerServer 创建执行单元查询服务。
func NewRunnerServer(catalog RunnerCatalog) *RunnerServer { return &RunnerServer{catalog: catalog} }

func (s *RunnerServer) FindRunnerByID(ctx context.Context, req *runnerv1.FindRunnerByIDRequest) (*runnerv1.FindRunnerByIDResponse, error) {
	r, err := s.catalog.FindByID(ctx, req.GetId())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &runnerv1.FindRunnerByIDResponse{Runner: toProtoRunner(r)}, nil
}

func (s *RunnerServer) ListRunnersByCodebookID(ctx context.Context,
	req *runnerv1.ListRunnersByCodebookIDRequest) (*runnerv1.ListRunnersByCodebookIDResponse, error) {
	runners, err := s.catalog.ListByCodebookID(ctx, req.GetCodebookId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := make([]*runnerv1.Runner, 0, len(runners))
	for _, r := range runners {
		out = append(out, toProtoRunner(r))
	}
	return &runnerv1.ListRunnersByCodebookIDResponse{Runners: out}, nil
}

// toProtoRunner 只暴露选择执行单元所需的元数据，脚本与变量不出 ecmdb-task。
func toProtoRunner(r *ecmdbrunnerv1.Runner) *runnerv1.Runner {
	return &runnerv1.Runner{
		Id: r.GetId(), Name: r.GetName(), CodebookId: r.GetCodebookId(), Kind: r.GetKind(),
		Target: r.GetTarget(), Handler: r.GetHandler(), Tags: r.GetTags(), Desc: r.GetDesc(),
		// eflow 用 program_kind 区分脚本来源；ECMDB 的执行单元都绑定 codebook 脚本
		ProgramKind: "CODEBOOK",
	}
}

// ExecutionServer 在原有 TaskExecutionService 实现上补充 GetTaskExecution：
// 嵌入原实现，其余接口行为不变；被终止的工作流执行以 CANCELLED 呈现。
type ExecutionServer struct {
	executorv1.TaskExecutionServiceServer
	executions ExecutionStore
	svc        Service
}

// NewExecutionServer 包装原 TaskExecutionService 实现。
func NewExecutionServer(base executorv1.TaskExecutionServiceServer, executions ExecutionStore, svc Service) *ExecutionServer {
	return &ExecutionServer{TaskExecutionServiceServer: base, executions: executions, svc: svc}
}

func (s *ExecutionServer) GetTaskExecution(ctx context.Context,
	req *executorv1.GetTaskExecutionRequest) (*executorv1.GetTaskExecutionResponse, error) {
	e, err := s.executions.FindByID(ctx, req.GetExecutionId())
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	st := toProtoStatus(e.Status)
	if e.Status.IsTerminalStatus() && !e.Status.IsSuccess() {
		if r, findErr := s.svc.FindByExecution(ctx, e.ID); findErr == nil && r.CancelRequested {
			st = executorv1.ExecutionStatus_CANCELLED
		}
	}
	return &executorv1.GetTaskExecutionResponse{Execution: &executorv1.TaskExecution{
		Id: e.ID, TaskId: e.Task.ID, TaskName: e.Task.Name, StartTime: e.StartTime, EndTime: e.EndTime,
		Status: st, RunningProgress: e.RunningProgress, ExecutorNodeId: e.ExecutorNodeID, TaskResult: e.TaskResult,
	}}, nil
}

func toProtoStatus(s domain.TaskExecutionStatus) executorv1.ExecutionStatus {
	switch s {
	case domain.TaskExecutionStatusRunning:
		return executorv1.ExecutionStatus_RUNNING
	case domain.TaskExecutionStatusSuccess:
		return executorv1.ExecutionStatus_SUCCESS
	case domain.TaskExecutionStatusFailed:
		return executorv1.ExecutionStatus_FAILED
	case domain.TaskExecutionStatusFailedRetryable:
		return executorv1.ExecutionStatus_FAILED_RETRYABLE
	case domain.TaskExecutionStatusFailedRescheduled:
		return executorv1.ExecutionStatus_FAILED_RESCHEDULABLE
	default:
		// PREPARE / WAITING_PULL 尚未开始执行
		return executorv1.ExecutionStatus_UNKNOWN
	}
}

// Register 注册工作流相关 gRPC 服务（TaskExecutionService 由调用方以 ExecutionServer 注册）。
func Register(server grpc.ServiceRegistrar, scheduler *SchedulerServer, runner *RunnerServer) {
	schedulerv1.RegisterSchedulerServiceServer(server, scheduler)
	runnerv1.RegisterRunnerServiceServer(server, runner)
}
