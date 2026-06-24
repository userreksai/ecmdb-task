package complete

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Duke1616/etask/internal/domain"
	"github.com/Duke1616/etask/internal/event"
	"github.com/Duke1616/etask/internal/service/acquirer"
	"github.com/Duke1616/etask/internal/service/task"
	"github.com/Duke1616/etask/internal/sse"
	"github.com/ecodeclub/mq-api"
)

const (
	number100             = 100
	number0               = 0
	completeRetryAttempts = 3
	completeRetryBackoff  = 100 * time.Millisecond
)

type Consumer struct {
	// 更新
	execSvc task.ExecutionService
	taskSvc task.Service
	acquire acquirer.TaskAcquirer
}

func NewConsumer(execSvc task.ExecutionService,
	taskSvc task.Service,
	acquirer acquirer.TaskAcquirer,
) *Consumer {
	return &Consumer{
		taskSvc: taskSvc,
		execSvc: execSvc,
		acquire: acquirer,
	}
}

func (c *Consumer) Consume(ctx context.Context, message *mq.Message) error {
	var evt event.Event
	err := json.Unmarshal(message.Value, &evt)
	if err != nil {
		return fmt.Errorf("序列化失败 %w", err)
	}

	return c.handleTask(ctx, evt)
}

func (c *Consumer) handleTask(ctx context.Context, evt event.Event) error {
	var err error
	if evt.ExecStatus.IsSuccess() {
		err = c.execSvc.UpdateScheduleResult(ctx, evt.ExecID, domain.TaskExecutionStatusSuccess,
			number100, time.Now().UnixMilli(), nil, evt.ExecNodeId, evt.TaskResult)
	} else {
		err = c.execSvc.UpdateScheduleResult(ctx, evt.ExecID, domain.TaskExecutionStatusFailed,
			number0, time.Now().UnixMilli(), nil, evt.ExecNodeId, evt.TaskResult)
	}
	if err != nil {
		return err
	}

	// 计算下次运行时间
	t, err := c.updateNextTimeWithRetry(ctx, evt.TaskID)
	if err != nil {
		return err
	}

	// 广播事件：实时刷新任务列表状态和下一次触发时间
	sse.GetSSEHub().Broadcast(sse.TaskStatusEvent{
		TaskID:   t.ID,
		Status:   t.Status.String(),
		NextTime: t.NextTime,
	})

	// 只有状态还是 PREEMPTED 的任务才需要释放
	// 一次性任务已经变为 INACTIVE，不需要释放
	if t.Status == domain.TaskStatusPreempted {
		return c.releaseWithRetry(ctx, evt.TaskID, evt.ScheduleNodeID)
	}

	return nil
}

func (c *Consumer) updateNextTimeWithRetry(ctx context.Context, taskID int64) (domain.Task, error) {
	var (
		t   domain.Task
		err error
	)
	for attempt := 0; attempt < completeRetryAttempts; attempt++ {
		t, err = c.taskSvc.UpdateNextTime(ctx, taskID)
		if err == nil {
			return t, nil
		}
		if waitErr := waitBeforeCompleteRetry(ctx, attempt); waitErr != nil {
			return domain.Task{}, waitErr
		}
	}
	return domain.Task{}, err
}

func (c *Consumer) releaseWithRetry(ctx context.Context, taskID int64, scheduleNodeID string) error {
	var err error
	for attempt := 0; attempt < completeRetryAttempts; attempt++ {
		err = c.acquire.Release(ctx, taskID, scheduleNodeID)
		if err == nil {
			return nil
		}
		if waitErr := waitBeforeCompleteRetry(ctx, attempt); waitErr != nil {
			return waitErr
		}
	}
	return err
}

func waitBeforeCompleteRetry(ctx context.Context, attempt int) error {
	if attempt >= completeRetryAttempts-1 {
		return nil
	}

	timer := time.NewTimer(time.Duration(attempt+1) * completeRetryBackoff)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
