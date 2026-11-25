package workflow

import (
	"context"
	"fmt"

	executorv1 "github.com/userreksai/ecmdb-task/api/proto/gen/etask/executor/v1"
	"github.com/userreksai/ecmdb-task/internal/domain"
	"github.com/userreksai/ecmdb-task/pkg/grpc/balancer"
	"github.com/userreksai/ecmdb-task/pkg/grpc/pool"
)

// grpcInterrupter 向执行该任务的执行器节点发送 Interrupt，并按返回状态更新执行记录
// （与 compensator.InterruptCompensator 的中断方式一致）。
type grpcInterrupter struct {
	clients    *pool.Clients[executorv1.ExecutorServiceClient]
	executions ExecutionStore
}

// NewInterrupter 创建执行器中断器。
func NewInterrupter(clients *pool.Clients[executorv1.ExecutorServiceClient], executions ExecutionStore) Interrupter {
	return &grpcInterrupter{clients: clients, executions: executions}
}

func (i *grpcInterrupter) Interrupt(ctx context.Context, execution domain.TaskExecution) error {
	if execution.Task.GrpcConfig == nil {
		return fmt.Errorf("执行记录 %d 缺少 gRPC 配置", execution.ID)
	}
	client := i.clients.Get(execution.Task.GrpcConfig.ServiceName)
	resp, err := client.Interrupt(balancer.WithSpecificNodeID(ctx, execution.ExecutorNodeID),
		&executorv1.InterruptRequest{Eid: execution.ID})
	if err != nil {
		return fmt.Errorf("发送中断请求失败: %w", err)
	}
	if !resp.GetSuccess() {
		return fmt.Errorf("执行器未能中断执行 %d", execution.ID)
	}
	state := domain.ExecutionStateFromProto(resp.GetExecutionState())
	if !state.Status.IsTerminalStatus() {
		state.Status = domain.TaskExecutionStatusFailed
	}
	if state.ID == 0 {
		state.ID, state.TaskID, state.TaskName = execution.ID, execution.Task.ID, execution.Task.Name
	}
	return i.executions.UpdateState(ctx, state)
}
