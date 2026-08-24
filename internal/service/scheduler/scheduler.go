package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/Duke1616/eiam/pkg/ctxutil"
	"github.com/gotomicro/ego/core/constant"
	"github.com/gotomicro/ego/core/elog"
	"github.com/gotomicro/ego/server"
	"github.com/userreksai/ecmdb-task/internal/domain"
	"github.com/userreksai/ecmdb-task/internal/service/acquirer"
	"github.com/userreksai/ecmdb-task/internal/service/picker"
	"github.com/userreksai/ecmdb-task/internal/service/runner"
	"github.com/userreksai/ecmdb-task/internal/service/task"
	"github.com/userreksai/ecmdb-task/pkg/grpc/balancer"
)

var _ server.Server = &Scheduler{}

// Scheduler 分布式任务调度器
type Scheduler struct {
	nodeID             string                    // 当前调度节点ID
	runner             runner.Runner             // Runner 分发器
	taskSvc            scheduleTaskService       // 任务服务
	execSvc            task.ExecutionService     // 任务执行服务
	acquirer           acquirer.TaskAcquirer     // 任务抢占、续约、释放器
	config             Config                    // 配置
	executorNodePicker picker.ExecutorNodePicker // 智能节点选择器
	modeResolver       picker.IExecModeResolver  // 执行模式感知器
	startedAt          int64                     // 本次调度器启动时间，用于跳过停机期间错过的周期触发
	ctx                context.Context
	cancel             context.CancelFunc
	logger             *elog.Component
}

type scheduleTaskService interface {
	SchedulableTasks(ctx context.Context, preemptedTimeoutMs int64, limit int) ([]domain.Task, error)
	UpdateNextTime(ctx context.Context, id int64) (domain.Task, error)
}

// Config 调度器配置
type Config struct {
	BatchTimeout     time.Duration `yaml:"batchTimeout"`
	BatchSize        int           `yaml:"batchSize"`        // 批量获取任务数量
	PreemptedTimeout time.Duration `yaml:"preemptedTimeout"` // 表示处于 PREEMPTED 状态任务的超时时间（毫秒）
	ScheduleInterval time.Duration `yaml:"scheduleInterval"` // 调度间隔
	RenewInterval    time.Duration `yaml:"renewInterval"`    // 续约间隔
}

// NewScheduler 创建调度器实例
func NewScheduler(
	nodeID string,
	runner runner.Runner,
	taskSvc task.Service,
	execSvc task.ExecutionService,
	acquirer acquirer.TaskAcquirer,
	config Config,
	executorNodePicker picker.ExecutorNodePicker,
	modeResolver picker.IExecModeResolver,
) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &Scheduler{
		nodeID:             nodeID,
		runner:             runner,
		taskSvc:            taskSvc,
		execSvc:            execSvc,
		acquirer:           acquirer,
		config:             config,
		executorNodePicker: executorNodePicker,
		modeResolver:       modeResolver,
		ctx:                ctx,
		cancel:             cancel,
		logger:             elog.DefaultLogger.With(elog.FieldComponentName("Scheduler")),
	}
}

func (s *Scheduler) NodeID() string {
	return s.nodeID
}

func (s *Scheduler) Name() string {
	return fmt.Sprintf("Scheduler-%s", s.nodeID)
}

func (s *Scheduler) PackageName() string {
	return "scheduler.Scheduler"
}

func (s *Scheduler) Init() error {
	return nil
}

// Start 启动调度器
func (s *Scheduler) Start() error {
	s.logger.Info("启动分布式任务调度器", elog.String("nodeID", s.nodeID))
	s.startedAt = time.Now().UnixMilli()

	// 启动调度循环
	go s.scheduleLoop()

	// 启动续约循环
	go s.renewLoop()
	return nil
}

// scheduleLoop 主调度循环
func (s *Scheduler) scheduleLoop() {
	for {
		if s.ctx.Err() != nil {
			s.logger.Info("调度循环结束")
			return
		}

		// 获取可调度的任务列表
		scheduleCtx, cancelFunc := context.WithTimeout(s.ctx, s.config.BatchTimeout)
		tasks, err := s.taskSvc.SchedulableTasks(scheduleCtx, s.config.PreemptedTimeout.Milliseconds(), s.config.BatchSize)
		cancelFunc()
		if err != nil {
			s.logger.Error("获取可调度任务失败", elog.FieldErr(err))
		}
		// 没有可以调度的任务就睡一会
		if len(tasks) == 0 {
			s.logger.Debug("没有可调度的任务")
			// 睡眠一下
			time.Sleep(s.config.ScheduleInterval)
			continue
		}

		s.logger.Info("发现可调度任务", elog.Int("count", len(tasks)))
		// 开始调度
		successCount := 0
		skippedCount := 0
		for i := range tasks {
			skipped, skipErr := s.skipMissedRecurringTask(tasks[i])
			if skipped {
				if skipErr != nil {
					s.logger.Error("跳过重启前错过的周期任务失败",
						elog.Int64("taskID", tasks[i].ID),
						elog.String("taskName", tasks[i].Name),
						elog.FieldErr(skipErr))
				} else {
					skippedCount++
				}
				continue
			}

			if err = s.scheduleOnce(tasks[i]); err != nil {
				s.logger.Error("调度任务失败",
					elog.Int64("taskID", tasks[i].ID),
					elog.String("taskName", tasks[i].Name),
					elog.FieldErr(err))
			} else {
				successCount++
			}
		}
		s.logger.Info("本次调度信息",
			elog.Int("success", successCount),
			elog.Int("skipped", skippedCount),
			elog.Int("total", len(tasks)))

	}
}

// skipMissedRecurringTask 跳过服务启动前已经到期的周期任务。
// 停机期间错过的触发点只推进 next_time，不补发执行；启动后的正常触发不受影响。
func (s *Scheduler) skipMissedRecurringTask(t domain.Task) (bool, error) {
	if !t.Type.IsRecurring() || t.NextTime >= s.startedAt {
		return false, nil
	}

	updated, err := s.taskSvc.UpdateNextTime(s.ctx, t.ID)
	if err != nil {
		// 即使推进失败也不能继续下发，否则仍会产生一次重启补跑。
		return true, fmt.Errorf("推进下次执行时间失败: %w", err)
	}

	s.logger.Info("已跳过重启前错过的周期任务",
		elog.Int64("taskID", t.ID),
		elog.String("taskName", t.Name),
		elog.Int64("missedNextTime", t.NextTime),
		elog.Int64("nextTime", updated.NextTime))
	return true, nil
}

// scheduleOnce 调度单个任务
func (s *Scheduler) scheduleOnce(task domain.Task) error {
	nodeID, err := s.executorNodePicker.Pick(s.ctx, task)
	if err != nil {
		s.logger.Error("智能调度选择节点失败，使用默认调度",
			elog.Int64("taskID", task.ID),
			elog.FieldErr(err))
	} else {
		s.logger.Info("智能调度选择节点成功",
			elog.String("selectedNodeID", nodeID),
			elog.Int64("taskID", task.ID))

		// 通过独立的 modeResolver 感知节点模式并写入 DB，
		// 调度器不封装任何 registry / DB 细节。
		task.ExecMode = s.modeResolver.ResolveMode(s.ctx, task, nodeID)
	}

	ctx := s.ctx
	// 注入租户上下文：让后续 DAO 操作（创建 TaskExecution 等）能被 TenantPlugin 自动填充
	if task.TenantID > 0 {
		ctx = ctxutil.WithTenantID(ctx, task.TenantID)
		ctx = ctxutil.WithOriginTenantID(ctx, task.TenantID)
	}
	if nodeID != "" {
		ctx = balancer.WithSpecificNodeID(ctx, nodeID)
	}
	return s.runner.Run(ctx, task)
}

// renewLoop 续约循环
func (s *Scheduler) renewLoop() {
	ticker := time.NewTicker(s.config.RenewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			err := s.acquirer.Renew(s.ctx, s.nodeID)
			if err != nil {
				s.logger.Error("批量续约失败", elog.FieldErr(err))
			}
		}
	}
}

// Stop 停止调度器
func (s *Scheduler) Stop() error {
	s.logger.Info("停止分布式任务调度器", elog.String("nodeID", s.nodeID))
	// 取消上下文
	s.cancel()
	return nil
}

func (s *Scheduler) GracefulStop(_ context.Context) error {
	s.logger.Info("停止分布式任务调度器", elog.String("nodeID", s.nodeID))
	s.cancel()
	return nil
}

func (s *Scheduler) Info() *server.ServiceInfo {
	info := server.ApplyOptions(
		server.WithName(s.Name()),
		server.WithKind(constant.ServiceProvider),
	)
	info.Healthy = s.ctx.Err() == nil
	return &info
}
