package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ecodeclub/mq-api"
	"github.com/gotomicro/ego/core/elog"
	agentevent "github.com/userreksai/ecmdb-task/internal/agent/event"
	"github.com/userreksai/ecmdb-task/internal/domain"
	mqx "github.com/userreksai/ecmdb-task/pkg/mpx"
)

// KAFKA 类型执行单元由 Kafka Agent 执行（Agent 代码不变）：
//
//	RunRunner ─► 一次性任务(INACTIVE) + 执行记录(PREPARE)
//	          ─► 投递 ExecuteReceive{task_id: -执行ID} 到执行单元配置的 Topic
//	Agent     ─► result_execute_events{task_id: -执行ID}
//	          ─► 本模块独立消费组：写执行日志、终结执行记录 ─► complete_topic ─► Bridge ─► eflow
//	超时巡检  ─► 超过最大执行时间仍无结果的执行标记为失败

const (
	// AgentResultTopic Kafka Agent 固定回传结果的 Topic
	AgentResultTopic = agentevent.ExecuteResultEventName
	// kafkaResultGroup 独立消费组：与 ECMDB 旧任务模块的消费组并行消费
	kafkaResultGroup = "eflow-kafka-result"
	expireBatchSize  = 100
)

// AgentMessage 投递给 Kafka Agent 的执行指令，字段与 internal/agent/event.ExecuteReceive 一致。
type AgentMessage = agentevent.ExecuteReceive

// AgentPublisher 向指定 Topic 投递 Agent 执行指令。
type AgentPublisher interface {
	Publish(ctx context.Context, topic string, msg AgentMessage) error
}

// LogWriter 写执行日志（由 internal/service/task.LogService 实现）。
type LogWriter interface {
	AddLog(ctx context.Context, log domain.TaskExecutionLog) error
}

// dispatchKafka 为 KAFKA 执行创建执行记录并投递指令，返回执行 ID。
// 投递成功后才标记 Dispatched；投递失败返回可重试错误，eflow 用同一 request_id 补发时会继续投递而不会重复创建执行。
func (s *service) dispatchKafka(ctx context.Context, r *Request, task domain.Task, exec RunnerExecution,
	req RunRequest) (int64, error) {
	if s.publisher == nil {
		return 0, fmt.Errorf("%w: 未启用 Kafka Agent 投递", ErrRejected)
	}
	if strings.TrimSpace(exec.Target) == "" {
		return 0, fmt.Errorf("%w: 执行单元 %d 未配置 Agent Topic", ErrRejected, exec.RunnerID)
	}

	if r.ExecutionID <= 0 {
		execution, err := s.executions.Create(ctx, domain.TaskExecution{
			Task: task, StartTime: time.Now().UnixMilli(), Status: domain.TaskExecutionStatusPrepare,
		})
		if err != nil {
			return 0, fmt.Errorf("创建执行记录失败: %w", err)
		}
		if err = s.dao.BindExecution(ctx, r.ID, execution.ID); err != nil {
			return 0, fmt.Errorf("绑定执行记录失败: %w", err)
		}
		r.ExecutionID = execution.ID
	}

	params := task.ScheduleParams
	if len(params) == 0 {
		var err error
		if params, err = buildParams(r.RequestID, exec, req); err != nil {
			return 0, err
		}
	}
	args := map[string]any{}
	if raw := strings.TrimSpace(params["args"]); raw != "" {
		if err := json.Unmarshal([]byte(raw), &args); err != nil {
			return 0, fmt.Errorf("%w: args 不是合法的 JSON 对象: %v", ErrInvalidRequest, err)
		}
	}
	msg := AgentMessage{
		TaskId:    KafkaCorrelationID(r.ExecutionID),
		Language:  exec.Language,
		Handler:   exec.Handler,
		Code:      params["code"],
		Args:      args,
		Variables: params["variables"],
	}
	if err := s.publisher.Publish(ctx, exec.Target, msg); err != nil {
		return 0, fmt.Errorf("投递 Kafka Agent 指令失败: %w", err)
	}

	maxSeconds := task.MaxExecutionSeconds
	if maxSeconds <= 0 {
		maxSeconds = domain.DefaultMaxExecutionSeconds
	}
	deadline := time.Now().Add(time.Duration(maxSeconds) * time.Second).UnixMilli()
	if err := s.dao.MarkKafkaDispatched(ctx, r.ID, deadline); err != nil {
		// 指令已投递，记录失败不影响执行；返回执行 ID，避免 eflow 补发造成重复执行
		s.logger.Error("记录 Kafka 指令已投递失败", elog.Int64("executionID", r.ExecutionID), elog.FieldErr(err))
	}
	r.Dispatched = true
	return r.ExecutionID, nil
}

// CompleteKafka 处理 Kafka Agent 回传的结果：写日志并终结执行记录（触发完成事件）。
func (s *service) CompleteKafka(ctx context.Context, executionID int64, success bool, output, result string) error {
	r, err := s.dao.FindByExecutionID(ctx, executionID)
	if errors.Is(err, ErrRequestNotFound) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("查询工作流请求失败: %w", err)
	}
	execution, err := s.executions.FindByID(ctx, executionID)
	if err != nil {
		return fmt.Errorf("查询执行记录失败: %w", err)
	}
	defer func() { _ = s.dao.ClearDeadline(ctx, r.ID) }()
	if execution.Status.IsTerminalStatus() {
		// 已被终止或超时终结：迟到的结果只记录日志
		s.logger.Info("忽略已终结执行的迟到结果", elog.Int64("executionID", executionID))
		return nil
	}

	if s.logs != nil && strings.TrimSpace(output) != "" {
		if logErr := s.logs.AddLog(ctx, domain.TaskExecutionLog{
			TaskID: execution.Task.ID, ExecutionID: executionID, Content: output, CTime: time.Now().UnixMilli(),
		}); logErr != nil {
			s.logger.Warn("写入 Kafka 执行日志失败", elog.Int64("executionID", executionID), elog.FieldErr(logErr))
		}
	}
	status := domain.TaskExecutionStatusFailed
	if success {
		status = domain.TaskExecutionStatusSuccess
	}
	return s.executions.UpdateState(ctx, domain.ExecutionState{
		ID: executionID, TaskID: execution.Task.ID, TaskName: execution.Task.Name,
		Status: status, RunningProgress: 100, TaskResult: result,
	})
}

// ExpireKafka 终结超过最大执行时间仍未收到结果的 KAFKA 执行。
func (s *service) ExpireKafka(ctx context.Context, now time.Time) (int, error) {
	expired, err := s.dao.ListExpired(ctx, now.UnixMilli(), expireBatchSize)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, r := range expired {
		execution, findErr := s.executions.FindByID(ctx, r.ExecutionID)
		if findErr == nil && !execution.Status.IsTerminalStatus() {
			if updErr := s.executions.UpdateState(ctx, domain.ExecutionState{
				ID: execution.ID, TaskID: execution.Task.ID, TaskName: execution.Task.Name,
				Status: domain.TaskExecutionStatusFailed, TaskResult: "Kafka Agent 执行超时，未收到执行结果",
			}); updErr != nil {
				s.logger.Error("终结超时 Kafka 执行失败", elog.Int64("executionID", execution.ID), elog.FieldErr(updErr))
				continue
			}
			count++
		}
		_ = s.dao.ClearDeadline(ctx, r.ID)
	}
	return count, nil
}

// mqAgentPublisher 基于 MQ 的 Agent 指令投递器，按 Topic 复用生产者。
type mqAgentPublisher struct {
	q         mq.MQ
	mu        sync.Mutex
	producers map[string]mq.Producer
}

// NewAgentPublisher 创建 Agent 指令投递器。
func NewAgentPublisher(q mq.MQ) AgentPublisher {
	return &mqAgentPublisher{q: q, producers: map[string]mq.Producer{}}
}

func (p *mqAgentPublisher) Publish(ctx context.Context, topic string, msg AgentMessage) error {
	producer, err := p.producer(topic)
	if err != nil {
		return err
	}
	value, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	m := &mq.Message{Value: value}
	mqx.InjectContext(ctx, m)
	_, err = producer.Produce(ctx, m)
	return err
}

func (p *mqAgentPublisher) producer(topic string) (mq.Producer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if producer, ok := p.producers[topic]; ok {
		return producer, nil
	}
	producer, err := p.q.Producer(topic)
	if err != nil {
		return nil, fmt.Errorf("创建 Topic %s 生产者失败: %w", topic, err)
	}
	p.producers[topic] = producer
	return producer, nil
}

// KafkaResultConsumer 消费 Kafka Agent 回传的结果，只处理工作流执行（task_id 为负数）。
type KafkaResultConsumer struct {
	consumer *mqx.Consumer
	svc      KafkaCompleter
	logger   *elog.Component
}

// KafkaCompleter 由 Service 实现。
type KafkaCompleter interface {
	CompleteKafka(ctx context.Context, executionID int64, success bool, output, result string) error
}

// NewKafkaResultConsumer 创建 Agent 结果消费者。
func NewKafkaResultConsumer(q mq.MQ, svc KafkaCompleter) *KafkaResultConsumer {
	return &KafkaResultConsumer{
		consumer: mqx.NewConsumer(fmt.Sprintf("%s-%s", AgentResultTopic, kafkaResultGroup), q, AgentResultTopic),
		svc:      svc,
		logger:   elog.DefaultLogger.With(elog.FieldComponentName("workflow.kafka.result")),
	}
}

// Start 启动消费（符合 ioc 后台任务约定）。
func (c *KafkaResultConsumer) Start(ctx context.Context) {
	if err := c.consumer.Start(ctx, c.Consume); err != nil {
		c.logger.Error("启动 Kafka Agent 结果消费者失败", elog.FieldErr(err))
	}
}

// Consume 处理一条 Agent 结果。
func (c *KafkaResultConsumer) Consume(ctx context.Context, msg *mq.Message) error {
	var evt agentevent.ExecuteResultEvent
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		return fmt.Errorf("解析 Agent 结果失败: %w", err)
	}
	executionID := ExecutionIDFromKafka(evt.TaskId)
	if executionID == 0 {
		return nil
	}
	return c.svc.CompleteKafka(ctx, executionID, evt.Status == agentevent.SUCCESS, evt.Result, evt.WantResult)
}

// KafkaExpireJob 定期终结超时的 KAFKA 执行。
type KafkaExpireJob struct {
	svc      Service
	interval time.Duration
	logger   *elog.Component
}

// NewKafkaExpireJob 创建超时巡检任务。
func NewKafkaExpireJob(svc Service, interval time.Duration) *KafkaExpireJob {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	return &KafkaExpireJob{svc: svc, interval: interval, logger: elog.DefaultLogger.With(elog.FieldComponentName("workflow.kafka.expire"))}
}

// Start 启动巡检循环（符合 ioc 后台任务约定）。
func (j *KafkaExpireJob) Start(ctx context.Context) {
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n, err := j.svc.ExpireKafka(ctx, time.Now()); err != nil {
				j.logger.Error("巡检超时 Kafka 执行失败", elog.FieldErr(err))
			} else if n > 0 {
				j.logger.Warn("已终结超时的 Kafka 执行", elog.Int("count", n))
			}
		}
	}
}
