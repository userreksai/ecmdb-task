package repository

import (
	"context"

	"github.com/ecodeclub/ekit/slice"
	"github.com/userreksai/ecmdb-task/internal/domain"
	"github.com/userreksai/ecmdb-task/internal/repository/dao"
)

type TaskCategoryRepository interface {
	Create(ctx context.Context, category domain.TaskCategory) (int64, error)
	FindByID(ctx context.Context, id int64) (domain.TaskCategory, error)
	List(ctx context.Context, bizID int64, keyword string) ([]domain.TaskCategory, error)
	CountUncategorized(ctx context.Context, bizID int64) (int64, error)
	Update(ctx context.Context, category domain.TaskCategory) (int64, error)
	Delete(ctx context.Context, id int64) (int64, error)
	ReplaceTasks(ctx context.Context, categoryID, bizID int64, taskIDs []int64) error
	ListTasks(ctx context.Context, bizID int64, categoryID *int64, query string, offset, limit int) ([]domain.Task, error)
	CountTasks(ctx context.Context, bizID int64, categoryID *int64, query string) (int64, error)
}

type taskCategoryRepository struct {
	dao dao.TaskCategoryDAO
}

func NewTaskCategoryRepository(categoryDAO dao.TaskCategoryDAO) TaskCategoryRepository {
	return &taskCategoryRepository{dao: categoryDAO}
}

func (r *taskCategoryRepository) Create(ctx context.Context, category domain.TaskCategory) (int64, error) {
	return r.dao.Create(ctx, r.toEntity(category))
}

func (r *taskCategoryRepository) FindByID(ctx context.Context, id int64) (domain.TaskCategory, error) {
	category, err := r.dao.FindByID(ctx, id)
	if err != nil {
		return domain.TaskCategory{}, err
	}
	return r.toDomain(category), nil
}

func (r *taskCategoryRepository) List(ctx context.Context, bizID int64, keyword string) ([]domain.TaskCategory, error) {
	categories, err := r.dao.List(ctx, bizID, keyword)
	if err != nil {
		return nil, err
	}
	return slice.Map(categories, func(_ int, category dao.TaskCategory) domain.TaskCategory {
		return r.toDomain(category)
	}), nil
}

func (r *taskCategoryRepository) CountUncategorized(ctx context.Context, bizID int64) (int64, error) {
	return r.dao.CountUncategorized(ctx, bizID)
}

func (r *taskCategoryRepository) Update(ctx context.Context, category domain.TaskCategory) (int64, error) {
	return r.dao.Update(ctx, r.toEntity(category))
}

func (r *taskCategoryRepository) Delete(ctx context.Context, id int64) (int64, error) {
	return r.dao.Delete(ctx, id)
}

func (r *taskCategoryRepository) ReplaceTasks(ctx context.Context, categoryID, bizID int64, taskIDs []int64) error {
	return r.dao.ReplaceTasks(ctx, categoryID, bizID, taskIDs)
}

func (r *taskCategoryRepository) ListTasks(
	ctx context.Context,
	bizID int64,
	categoryID *int64,
	query string,
	offset, limit int,
) ([]domain.Task, error) {
	tasks, err := r.dao.ListTasks(ctx, bizID, categoryID, query, offset, limit)
	if err != nil {
		return nil, err
	}
	return slice.Map(tasks, func(_ int, task *dao.Task) domain.Task {
		return taskEntityToDomain(task)
	}), nil
}

func (r *taskCategoryRepository) CountTasks(
	ctx context.Context,
	bizID int64,
	categoryID *int64,
	query string,
) (int64, error) {
	return r.dao.CountTasks(ctx, bizID, categoryID, query)
}

func (r *taskCategoryRepository) toEntity(category domain.TaskCategory) dao.TaskCategory {
	return dao.TaskCategory{
		ID:          category.ID,
		TenantID:    category.TenantID,
		Name:        category.Name,
		Description: category.Description,
		TaskCount:   category.TaskCount,
		CTime:       category.CTime,
		UTime:       category.UTime,
	}
}

func (r *taskCategoryRepository) toDomain(category dao.TaskCategory) domain.TaskCategory {
	return domain.TaskCategory{
		ID:          category.ID,
		TenantID:    category.TenantID,
		Name:        category.Name,
		Description: category.Description,
		TaskCount:   category.TaskCount,
		CTime:       category.CTime,
		UTime:       category.UTime,
	}
}
