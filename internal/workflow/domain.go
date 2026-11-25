// Package workflow 为 eflow（工单流程编排）提供上游 etask 新版调度协议的兼容实现：
//
//   - etask.scheduler.v1.SchedulerService：RunRunner / TerminateExecution
//   - etask.runner.v1.RunnerService：执行单元查询（代理到 ECMDB）
//   - etask.executor.v1.TaskExecutionService.GetTaskExecution：按执行 ID 对账
//   - Kafka task_execution_complete_events：工作流执行终态回传
//
// 设计原则：只做"协议适配"，执行仍走 ecmdb-task 现有的 一次性任务 → 调度 → 执行器 → 补偿 链路，
// 不改动原有任务管理、分类、SSE、Agent 等能力。新增状态全部记录在 workflow_requests 表中，
// 避免修改 tasks / task_executions 的表结构与状态枚举。
package workflow

import "strings"

// Source 工作流执行的来源。
type Source string

const (
	// SourceWorkflow eflow 自动化节点：终态需要回传给 eflow
	SourceWorkflow Source = "WORKFLOW"
	// SourceNotify eflow 通知脚本：尽力而为，不回传终态
	SourceNotify Source = "NOTIFY"

	// notifyRequestPrefix 与 eflow internal/pkg/notification/provider/script.RequestIDPrefix 保持一致
	notifyRequestPrefix = "notify:"
)

// SourceOf 根据请求标识判断来源。
func SourceOf(requestID string) Source {
	if strings.HasPrefix(requestID, notifyRequestPrefix) {
		return SourceNotify
	}
	return SourceWorkflow
}

// Request 一次工作流执行请求（以 request_id 幂等）。
type Request struct {
	ID              int64
	TenantID        int64
	RequestID       string
	RunnerID        int64
	TaskID          int64
	ExecutionID     int64
	Source          Source
	CancelRequested bool
	CancelReason    string
	// Kind 执行单元运行模式（GRPC / KAFKA），KAFKA 执行不经过调度循环
	Kind string
	// Dispatched KAFKA 执行的指令是否已投递到 Agent Topic（避免补发时重复执行）
	Dispatched bool
	// Deadline KAFKA 执行的超时时间点（毫秒）；0 表示已结束或无需超时巡检
	Deadline int64
	CTime    int64
	UTime    int64
}

// IsKafka 是否为 Kafka Agent 执行。
func (r Request) IsKafka() bool { return r.Kind == RunnerKindKafka }

// KafkaCorrelationID 投递给 Kafka Agent 的 task_id：使用执行 ID 的相反数。
//
// 为什么用负数：Agent 把结果固定回传到 result_execute_events，ECMDB 旧的自动化任务模块也在消费该 Topic，
// 并按 task_id 更新自己的任务。负数不会与 ECMDB 的任务 ID 冲突，两边可以安全共存。
func KafkaCorrelationID(executionID int64) int64 { return -executionID }

// ExecutionIDFromKafka 从 Agent 回传的 task_id 还原执行 ID；非工作流执行返回 0。
func ExecutionIDFromKafka(taskID int64) int64 {
	if taskID >= 0 {
		return 0
	}
	return -taskID
}

// RunRequest RunRunner 的入参。
type RunRequest struct {
	RequestID           string
	RunnerID            int64
	Params              map[string]string
	Variables           map[string]string
	MaxExecutionSeconds int64
}

// RunResult RunRunner 的结果。
type RunResult struct {
	ExecutionID int64
	Created     bool
}

// RunnerExecution 一次执行所需的执行单元信息（来自 ECMDB）。
type RunnerExecution struct {
	RunnerID   int64
	Name       string
	CodebookID int64
	Kind       string
	Target     string
	Handler    string
	Code       string
	Language   string
	Variables  []Variable
}

// Variable 执行变量，序列化格式与执行器 SDK（sdk/executor.Variable）一致。
type Variable struct {
	Key    string `json:"key"`
	Value  string `json:"value"`
	Secret bool   `json:"secret"`
}

const (
	// RunnerKindGRPC 由 ecmdb-task 执行器（PUSH/PULL）执行
	RunnerKindGRPC = "GRPC"
	// RunnerKindKafka 由 Kafka Agent 执行
	RunnerKindKafka = "KAFKA"
)
