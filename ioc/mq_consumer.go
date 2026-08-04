package ioc

import (
	"context"
	"fmt"

	"github.com/userreksai/ecmdb-task/internal/event/complete"
	"github.com/userreksai/ecmdb-task/internal/service/acquirer"
	"github.com/userreksai/ecmdb-task/internal/service/task"
	mqx "github.com/userreksai/ecmdb-task/pkg/mpx"
	"github.com/ecodeclub/mq-api"
)

func InitCompleteEventConsumer(q mq.MQ,
	taskSvc task.Service,
	execSvc task.ExecutionService,
	acquire acquirer.TaskAcquirer,
) *CompleteConsumer {
	topic := "complete_topic"
	group := "reporter"
	con := mqx.NewConsumer(name(topic, group), q, topic)
	comConsumer := complete.NewConsumer(execSvc, taskSvc, acquire)
	return &CompleteConsumer{
		com:      con,
		Consumer: comConsumer,
	}
}

type CompleteConsumer struct {
	*complete.Consumer
	com *mqx.Consumer
}

func (c *CompleteConsumer) Start(ctx context.Context) {
	err := c.com.Start(ctx, c.Consume)
	if err != nil {
		panic(err)
	}
}

func name(eventName, group string) string {
	return fmt.Sprintf("%s-%s", eventName, group)
}
