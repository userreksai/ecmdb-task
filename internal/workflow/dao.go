package workflow

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ErrRequestNotFound 请求记录不存在。
var ErrRequestNotFound = errors.New("工作流执行请求不存在")

// WorkflowRequest workflow_requests 表模型。
type WorkflowRequest struct {
	ID              int64  `gorm:"type:bigint;primaryKey;autoIncrement"`
	TenantID        int64  `gorm:"type:bigint unsigned;not null;default:0;uniqueIndex:uniq_tenant_request,priority:1;comment:'租户ID'"`
	RequestID       string `gorm:"type:varchar(128);not null;uniqueIndex:uniq_tenant_request,priority:2;comment:'eflow 幂等请求标识'"`
	RunnerID        int64  `gorm:"type:bigint;not null;default:0;comment:'执行单元ID（ECMDB）'"`
	TaskID          int64  `gorm:"type:bigint;not null;default:0;index;comment:'ecmdb-task 一次性任务ID'"`
	ExecutionID     int64  `gorm:"type:bigint;not null;default:0;index;comment:'执行记录ID'"`
	Source          string `gorm:"type:varchar(32);not null;default:'WORKFLOW';comment:'WORKFLOW|NOTIFY'"`
	CancelRequested bool   `gorm:"not null;default:false;comment:'是否已请求终止'"`
	CancelReason    string `gorm:"type:varchar(1000);not null;default:'';comment:'终止原因'"`
	Ctime           int64
	Utime           int64
}

// TableName 指定表名。
func (WorkflowRequest) TableName() string { return "workflow_requests" }

// DAO 工作流请求存储。
type DAO interface {
	// FindOrCreate 按 request_id 幂等创建，返回记录及是否新建
	FindOrCreate(ctx context.Context, r Request) (Request, bool, error)
	FindByRequestID(ctx context.Context, requestID string) (Request, error)
	FindByTaskID(ctx context.Context, taskID int64) (Request, error)
	FindByExecutionID(ctx context.Context, executionID int64) (Request, error)
	BindTask(ctx context.Context, id, taskID int64) error
	BindExecution(ctx context.Context, id, executionID int64) error
	MarkCancel(ctx context.Context, id int64, reason string) error
}

type gormDAO struct{ db *gorm.DB }

// NewDAO 创建 GORM 存储。
func NewDAO(db *gorm.DB) DAO { return &gormDAO{db: db} }

// InitTables 创建 workflow_requests 表（AutoMigrate，与 ecmdb-task 现有表的初始化方式一致）。
func InitTables(db *gorm.DB) error { return db.AutoMigrate(&WorkflowRequest{}) }

func (d *gormDAO) FindOrCreate(ctx context.Context, r Request) (Request, bool, error) {
	now := time.Now().UnixMilli()
	row := toModel(r)
	row.Ctime, row.Utime = now, now
	res := d.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&row)
	if res.Error != nil {
		return Request{}, false, res.Error
	}
	if res.RowsAffected == 1 {
		return toDomain(row), true, nil
	}
	existing, err := d.FindByRequestID(ctx, r.RequestID)
	return existing, false, err
}

func (d *gormDAO) FindByRequestID(ctx context.Context, requestID string) (Request, error) {
	return d.findOne(ctx, "request_id = ?", requestID)
}

func (d *gormDAO) FindByTaskID(ctx context.Context, taskID int64) (Request, error) {
	return d.findOne(ctx, "task_id = ?", taskID)
}

func (d *gormDAO) FindByExecutionID(ctx context.Context, executionID int64) (Request, error) {
	return d.findOne(ctx, "execution_id = ?", executionID)
}

func (d *gormDAO) findOne(ctx context.Context, query string, arg any) (Request, error) {
	var row WorkflowRequest
	err := d.db.WithContext(ctx).Where(query, arg).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return Request{}, ErrRequestNotFound
	}
	if err != nil {
		return Request{}, err
	}
	return toDomain(row), nil
}

func (d *gormDAO) BindTask(ctx context.Context, id, taskID int64) error {
	return d.update(ctx, id, map[string]any{"task_id": taskID})
}

func (d *gormDAO) BindExecution(ctx context.Context, id, executionID int64) error {
	return d.update(ctx, id, map[string]any{"execution_id": executionID})
}

func (d *gormDAO) MarkCancel(ctx context.Context, id int64, reason string) error {
	return d.update(ctx, id, map[string]any{"cancel_requested": true, "cancel_reason": reason})
}

func (d *gormDAO) update(ctx context.Context, id int64, values map[string]any) error {
	values["utime"] = time.Now().UnixMilli()
	return d.db.WithContext(ctx).Model(&WorkflowRequest{}).Where("id = ?", id).Updates(values).Error
}

func toModel(r Request) WorkflowRequest {
	return WorkflowRequest{
		ID: r.ID, TenantID: r.TenantID, RequestID: r.RequestID, RunnerID: r.RunnerID,
		TaskID: r.TaskID, ExecutionID: r.ExecutionID, Source: string(r.Source),
		CancelRequested: r.CancelRequested, CancelReason: r.CancelReason,
	}
}

func toDomain(m WorkflowRequest) Request {
	return Request{
		ID: m.ID, TenantID: m.TenantID, RequestID: m.RequestID, RunnerID: m.RunnerID,
		TaskID: m.TaskID, ExecutionID: m.ExecutionID, Source: Source(m.Source),
		CancelRequested: m.CancelRequested, CancelReason: m.CancelReason,
		CTime: m.Ctime, UTime: m.Utime,
	}
}
