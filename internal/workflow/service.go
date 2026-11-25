package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gotomicro/ego/core/elog"
	"github.com/userreksai/ecmdb-task/internal/domain"
	"github.com/userreksai/ecmdb-task/internal/errs"
)

var (
	// ErrInvalidRequest 请求参数非法（映射为 gRPC InvalidArgument，eflow 不会重试）
	ErrInvalidRequest = errors.New("工作流执行请求参数非法")
	// ErrRejected 请求被拒绝（映射为 FailedPrecondition，eflow 不会重试）
	ErrRejected = errors.New("工作流执行请求被拒绝")
	// ErrExecutionPending 执行记录尚未生成（映射为 Unavailable，eflow 会用同一 request_id 幂等重试）
	ErrExecutionPending = errors.New("执行记录尚未生成，请稍后重试")
)

const (
	taskNamePrefix      = "eflow-"
	executionWaitStep   = 100 * time.Millisecond
	defaultWaitDuration = 3 * time.Second
)

// TaskCreator 创建一次性任务（由 internal/service/task.Service 实现）。
type TaskCreator interface {
	Create(ctx context.Context, task domain.Task) (domain.Task, error)
	GetByID(ctx context.Context, id int64) (domain.Task, error)
	Stop(ctx context.Context, id int64) error
}

// ExecutionStore 执行记录读写（由 internal/service/task.ExecutionService 实现）。
type ExecutionStore interface {
	FindByID(ctx context.Context, id int64) (domain.TaskExecution, error)
	ListByTaskID(ctx context.Context, taskID int64, offset, limit int) ([]domain.TaskExecution, int64, error)
	UpdateState(ctx context.Context, state domain.ExecutionState) error
}

// ImmediateScheduler 立即调度一个任务（由 internal/service/scheduler.Scheduler 实现）。
type ImmediateScheduler interface {
	ScheduleNow(ctx context.Context, task domain.Task) error
}

// Interrupter 中断运行中的执行（向执行器发送 Interrupt）。
type Interrupter interface {
	Interrupt(ctx context.Context, execution domain.TaskExecution) error
}

// Service 工作流执行编排。
type Service interface {
	RunRunner(ctx context.Context, req RunRequest) (RunResult, error)
	TerminateExecution(ctx context.Context, executionID int64, requestID, reason string) error
	// FindByExecution 返回执行对应的工作流请求（不存在时返回 ErrRequestNotFound）
	FindByExecution(ctx context.Context, executionID int64) (Request, error)
	// FindByTask 返回任务对应的工作流请求（不存在时返回 ErrRequestNotFound）
	FindByTask(ctx context.Context, taskID int64) (Request, error)
}

type service struct {
	dao         DAO
	runners     RunnerCatalog
	tasks       TaskCreator
	executions  ExecutionStore
	scheduler   ImmediateScheduler
	interrupter Interrupter
	waitFor     time.Duration
	logger      *elog.Component
}

// NewService 创建工作流执行编排服务。
func NewService(dao DAO, runners RunnerCatalog, tasks TaskCreator, executions ExecutionStore,
	scheduler ImmediateScheduler, interrupter Interrupter) Service {
	return &service{
		dao: dao, runners: runners, tasks: tasks, executions: executions,
		scheduler: scheduler, interrupter: interrupter, waitFor: defaultWaitDuration,
		logger: elog.DefaultLogger.With(elog.FieldComponentName("workflow.service")),
	}
}

func (s *service) RunRunner(ctx context.Context, req RunRequest) (RunResult, error) {
	req.RequestID = strings.TrimSpace(req.RequestID)
	if req.RequestID == "" || req.RunnerID <= 0 {
		return RunResult{}, fmt.Errorf("%w: request_id 与 runner_id 不能为空", ErrInvalidRequest)
	}

	r, created, err := s.dao.FindOrCreate(ctx, Request{
		RequestID: req.RequestID, RunnerID: req.RunnerID, Source: SourceOf(req.RequestID),
	})
	if err != nil {
		return RunResult{}, fmt.Errorf("保存工作流执行请求失败: %w", err)
	}
	if r.CancelRequested {
		return RunResult{}, fmt.Errorf("%w: 请求 %s 已被终止", ErrRejected, r.RequestID)
	}
	if r.ExecutionID > 0 {
		return RunResult{ExecutionID: r.ExecutionID, Created: false}, nil
	}

	task, err := s.ensureTask(ctx, &r, req)
	if err != nil {
		return RunResult{}, err
	}

	// 立即调度：与调度循环竞争时由任务乐观锁保证只抢占一次，抢占失败说明调度循环已接手
	if err = s.scheduler.ScheduleNow(ctx, task); err != nil && !errors.Is(err, errs.ErrTaskPreemptFailed) {
		s.logger.Warn("立即调度失败，等待调度循环接手", elog.Int64("taskID", task.ID), elog.FieldErr(err))
	}

	executionID, err := s.waitExecution(ctx, task.ID)
	if err != nil {
		return RunResult{}, err
	}
	if err = s.dao.BindExecution(ctx, r.ID, executionID); err != nil {
		return RunResult{}, fmt.Errorf("绑定执行记录失败: %w", err)
	}
	return RunResult{ExecutionID: executionID, Created: created}, nil
}

// ensureTask 为请求创建（或找回已创建的）一次性任务。
func (s *service) ensureTask(ctx context.Context, r *Request, req RunRequest) (domain.Task, error) {
	if r.TaskID > 0 {
		return s.tasks.GetByID(ctx, r.TaskID)
	}

	exec, err := s.runners.ResolveExecution(ctx, req.RunnerID)
	if err != nil {
		return domain.Task{}, fmt.Errorf("解析执行单元 %d 失败: %w", req.RunnerID, err)
	}
	if !strings.EqualFold(exec.Kind, RunnerKindGRPC) {
		return domain.Task{}, fmt.Errorf("%w: 执行单元 %d 的运行模式为 %s，工作流执行目前只支持 GRPC 执行器",
			ErrRejected, req.RunnerID, exec.Kind)
	}

	params, err := buildParams(r.RequestID, exec, req)
	if err != nil {
		return domain.Task{}, err
	}
	task, err := s.tasks.Create(ctx, domain.Task{
		Name:                taskNamePrefix + r.RequestID,
		BizKey:              r.RequestID,
		Type:                domain.TaskTypeOneTime,
		MaxExecutionSeconds: req.MaxExecutionSeconds,
		GrpcConfig:          &domain.GrpcConfig{ServiceName: exec.Target, HandlerName: exec.Handler, Params: params},
		// 工作流执行的重试由 eflow 以新的执行尝试发起，这里不做执行器层重试，避免重复执行外部动作
		RetryConfig: &domain.RetryConfig{MaxRetries: 0},
		Status:      domain.TaskStatusActive,
		Version:     1,
		Metadata: map[string]string{
			"source": string(r.Source), "request_id": r.RequestID,
			"runner_id": strconv.FormatInt(req.RunnerID, 10), "runner_name": exec.Name,
		},
	})
	if err != nil {
		return domain.Task{}, fmt.Errorf("创建一次性任务失败: %w", err)
	}
	if err = s.dao.BindTask(ctx, r.ID, task.ID); err != nil {
		return domain.Task{}, fmt.Errorf("绑定一次性任务失败: %w", err)
	}
	r.TaskID = task.ID
	return task, nil
}

// buildParams 组装执行器参数：与 ECMDB 原有下发格式一致（code / args / variables），
// 请求中的变量覆盖执行单元同名变量。
func buildParams(requestID string, exec RunnerExecution, req RunRequest) (map[string]string, error) {
	params := make(map[string]string, len(req.Params)+4)
	for k, v := range req.Params {
		params[k] = v
	}
	vars := make([]Variable, 0, len(exec.Variables)+len(req.Variables))
	overridden := make(map[string]bool, len(req.Variables))
	for k, v := range req.Variables {
		vars = append(vars, Variable{Key: k, Value: v})
		overridden[k] = true
	}
	for _, v := range exec.Variables {
		if !overridden[v.Key] {
			vars = append(vars, v)
		}
	}
	raw, err := json.Marshal(vars)
	if err != nil {
		return nil, fmt.Errorf("序列化执行变量失败: %w", err)
	}
	params["code"] = exec.Code
	params["variables"] = string(raw)
	params["request_id"] = requestID
	if exec.Language != "" {
		params["language"] = exec.Language
	}
	if _, ok := params["args"]; !ok {
		params["args"] = "{}"
	}
	return params, nil
}

// waitExecution 等待调度链路为任务创建执行记录（创建发生在抢占后，通常是毫秒级）。
func (s *service) waitExecution(ctx context.Context, taskID int64) (int64, error) {
	deadline := time.Now().Add(s.waitFor)
	for {
		executions, _, err := s.executions.ListByTaskID(ctx, taskID, 0, 1)
		if err == nil && len(executions) > 0 && executions[0].ID > 0 {
			return executions[0].ID, nil
		}
		if time.Now().After(deadline) {
			return 0, fmt.Errorf("%w: task_id=%d", ErrExecutionPending, taskID)
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(executionWaitStep):
		}
	}
}

func (s *service) TerminateExecution(ctx context.Context, executionID int64, requestID, reason string) error {
	r, err := s.locate(ctx, executionID, requestID)
	if err != nil {
		return err
	}
	if !r.CancelRequested {
		if err = s.dao.MarkCancel(ctx, r.ID, reason); err != nil {
			return fmt.Errorf("记录终止意图失败: %w", err)
		}
	}

	// 还没有执行记录：停用任务，阻止后续调度（RunRunner 也会因终止标记拒绝再次提交）
	if r.ExecutionID <= 0 {
		if r.TaskID > 0 {
			if err = s.tasks.Stop(ctx, r.TaskID); err != nil {
				return fmt.Errorf("停用一次性任务失败: %w", err)
			}
		}
		return nil
	}

	execution, err := s.executions.FindByID(ctx, r.ExecutionID)
	if err != nil {
		return fmt.Errorf("查询执行记录失败: %w", err)
	}
	if execution.Status.IsTerminalStatus() {
		return nil
	}
	if execution.Status.IsRunning() && s.interrupter != nil {
		if err = s.interrupter.Interrupt(ctx, execution); err == nil {
			return nil
		}
		s.logger.Warn("中断执行器失败，直接终结执行记录", elog.Int64("executionID", execution.ID), elog.FieldErr(err))
	}
	// 尚未开始（PREPARE / WAITING_PULL）或执行器已不可达：直接终结，触发完成事件
	return s.executions.UpdateState(ctx, domain.ExecutionState{
		ID: execution.ID, TaskID: execution.Task.ID, TaskName: execution.Task.Name,
		Status: domain.TaskExecutionStatusFailed, TaskResult: "已被终止: " + reason,
	})
}

func (s *service) locate(ctx context.Context, executionID int64, requestID string) (Request, error) {
	if executionID > 0 {
		r, err := s.dao.FindByExecutionID(ctx, executionID)
		if err == nil || !errors.Is(err, ErrRequestNotFound) || strings.TrimSpace(requestID) == "" {
			return r, wrapNotFound(err)
		}
	}
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return Request{}, fmt.Errorf("%w: execution_id 与 request_id 不能同时为空", ErrInvalidRequest)
	}
	// eflow 可能在提交请求尚未到达（网络失败）时就发起终止：此时先落一条带终止标记的请求，
	// 保证之后用同一 request_id 的补发会被拒绝，满足"取消意图已持久化"的语义
	r, _, err := s.dao.FindOrCreate(ctx, Request{RequestID: requestID, Source: SourceOf(requestID)})
	if err != nil {
		return Request{}, fmt.Errorf("查询工作流执行请求失败: %w", err)
	}
	return r, nil
}

func wrapNotFound(err error) error {
	if errors.Is(err, ErrRequestNotFound) {
		return fmt.Errorf("%w: %v", ErrRejected, err)
	}
	return err
}

func (s *service) FindByExecution(ctx context.Context, executionID int64) (Request, error) {
	return s.dao.FindByExecutionID(ctx, executionID)
}

func (s *service) FindByTask(ctx context.Context, taskID int64) (Request, error) {
	return s.dao.FindByTaskID(ctx, taskID)
}
