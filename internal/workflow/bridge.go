package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ecodeclub/mq-api"
	"github.com/gotomicro/ego/core/elog"
	"github.com/userreksai/ecmdb-task/internal/event"
	mqx "github.com/userreksai/ecmdb-task/pkg/mpx"
)

const (
	// CompleteTopic ecmdb-task 内部执行完成事件
	CompleteTopic = "complete_topic"
	// EflowResultTopic eflow 消费的工作流执行终态事件（与 eflow internal/event.ExecuteResultEventName 一致）
	EflowResultTopic = "task_execution_complete_events"
	// bridgeGroup 独立消费组：与原有 reporter 组并行消费，互不影响
	bridgeGroup = "eflow-bridge"
)

// ResultEvent eflow 的执行终态事件结构（字段名与 eflow internal/event.ExecuteResultEvent 一致）。
type ResultEvent struct {
	TaskID     int64  `json:"taskId"`
	ExecID     int64  `json:"execId"`
	ExecStatus string `json:"execStatus"` // SUCCESS | FAILED | CANCELLED
	TaskResult string `json:"taskResult"`
	Source     string `json:"source"`
	RequestID  string `json:"requestId"`
}

// Bridge 把工作流执行的完成事件转发给 eflow。非工作流任务与通知脚本执行直接忽略。
type Bridge struct {
	consumer *mqx.Consumer
	producer mqx.Producer[ResultEvent]
	svc      Service
	logger   *elog.Component
}

// NewBridge 创建完成事件桥接器。
func NewBridge(q mq.MQ, svc Service) (*Bridge, error) {
	producer, err := mqx.NewGeneralProducer[ResultEvent](q, EflowResultTopic)
	if err != nil {
		return nil, fmt.Errorf("创建 eflow 结果事件生产者失败: %w", err)
	}
	return &Bridge{
		consumer: mqx.NewConsumer(fmt.Sprintf("%s-%s", CompleteTopic, bridgeGroup), q, CompleteTopic),
		producer: producer,
		svc:      svc,
		logger:   elog.DefaultLogger.With(elog.FieldComponentName("workflow.bridge")),
	}, nil
}

// Start 启动消费（符合 ioc 后台任务约定）。
func (b *Bridge) Start(ctx context.Context) {
	if err := b.consumer.Start(ctx, b.Consume); err != nil {
		b.logger.Error("启动 eflow 结果桥接消费者失败", elog.FieldErr(err))
	}
}

// Consume 处理一条内部完成事件。
func (b *Bridge) Consume(ctx context.Context, msg *mq.Message) error {
	var evt event.Event
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		return fmt.Errorf("解析完成事件失败: %w", err)
	}
	out, ok, err := b.Translate(ctx, evt)
	if err != nil || !ok {
		return err
	}
	return b.producer.Produce(ctx, out)
}

// Translate 将内部完成事件转换为 eflow 结果事件；ok=false 表示无需转发。
func (b *Bridge) Translate(ctx context.Context, evt event.Event) (ResultEvent, bool, error) {
	r, err := b.svc.FindByTask(ctx, evt.TaskID)
	if errors.Is(err, ErrRequestNotFound) {
		return ResultEvent{}, false, nil
	}
	if err != nil {
		return ResultEvent{}, false, fmt.Errorf("查询工作流请求失败: %w", err)
	}
	if r.Source != SourceWorkflow {
		return ResultEvent{}, false, nil
	}

	execStatus := "FAILED"
	switch {
	case evt.ExecStatus.IsSuccess():
		execStatus = "SUCCESS"
	case r.CancelRequested:
		execStatus = "CANCELLED"
	}
	return ResultEvent{
		TaskID: evt.TaskID, ExecID: evt.ExecID, ExecStatus: execStatus,
		TaskResult: evt.TaskResult, Source: string(SourceWorkflow), RequestID: r.RequestID,
	}, true, nil
}
