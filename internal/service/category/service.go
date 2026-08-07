package category

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/userreksai/ecmdb-task/internal/domain"
	"github.com/userreksai/ecmdb-task/internal/repository"
	"github.com/userreksai/ecmdb-task/pkg/grpc/interceptors/bizid"
	"golang.org/x/sync/errgroup"
)

type Service interface {
	Create(ctx context.Context, category domain.TaskCategory) (int64, error)
	List(ctx context.Context, keyword string) ([]domain.TaskCategory, int64, error)
	Update(ctx context.Context, category domain.TaskCategory) (int64, error)
	Delete(ctx context.Context, id int64) (int64, error)
	ReplaceTasks(ctx context.Context, categoryID int64, taskIDs []int64) error
	ListTasks(ctx context.Context, categoryID *int64, query string, offset, limit int) ([]domain.Task, int64, error)
}

type service struct {
	repo repository.TaskCategoryRepository
}

func NewService(repo repository.TaskCategoryRepository) Service {
	return &service{repo: repo}
}

func (s *service) Create(ctx context.Context, category domain.TaskCategory) (int64, error) {
	if err := normalizeAndValidate(&category); err != nil {
		return 0, err
	}
	return s.repo.Create(ctx, category)
}

func (s *service) List(ctx context.Context, keyword string) ([]domain.TaskCategory, int64, error) {
	var (
		categories   []domain.TaskCategory
		unclassified int64
	)
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		var err error
		categories, err = s.repo.List(ctx, bizid.Task, strings.TrimSpace(keyword))
		return err
	})
	eg.Go(func() error {
		var err error
		unclassified, err = s.repo.CountUncategorized(ctx, bizid.Task)
		return err
	})
	if err := eg.Wait(); err != nil {
		return nil, 0, err
	}
	return categories, unclassified, nil
}

func (s *service) Update(ctx context.Context, category domain.TaskCategory) (int64, error) {
	if category.ID <= 0 {
		return 0, fmt.Errorf("category id must be greater than zero")
	}
	if err := normalizeAndValidate(&category); err != nil {
		return 0, err
	}
	return s.repo.Update(ctx, category)
}

func (s *service) Delete(ctx context.Context, id int64) (int64, error) {
	if id <= 0 {
		return 0, fmt.Errorf("category id must be greater than zero")
	}
	return s.repo.Delete(ctx, id)
}

func (s *service) ReplaceTasks(ctx context.Context, categoryID int64, taskIDs []int64) error {
	if categoryID <= 0 {
		return fmt.Errorf("category id must be greater than zero")
	}
	unique := make([]int64, 0, len(taskIDs))
	seen := make(map[int64]struct{}, len(taskIDs))
	for _, taskID := range taskIDs {
		if taskID <= 0 {
			return fmt.Errorf("task id must be greater than zero")
		}
		if _, ok := seen[taskID]; ok {
			continue
		}
		seen[taskID] = struct{}{}
		unique = append(unique, taskID)
	}
	return s.repo.ReplaceTasks(ctx, categoryID, bizid.Task, unique)
}

func (s *service) ListTasks(
	ctx context.Context,
	categoryID *int64,
	query string,
	offset, limit int,
) ([]domain.Task, int64, error) {
	if categoryID != nil && *categoryID < 0 {
		return nil, 0, fmt.Errorf("category id cannot be negative")
	}
	if offset < 0 || limit < 0 {
		return nil, 0, fmt.Errorf("offset and limit cannot be negative")
	}
	query = strings.TrimSpace(query)
	var (
		tasks []domain.Task
		total int64
	)
	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		var err error
		tasks, err = s.repo.ListTasks(ctx, bizid.Task, categoryID, query, offset, limit)
		return err
	})
	eg.Go(func() error {
		var err error
		total, err = s.repo.CountTasks(ctx, bizid.Task, categoryID, query)
		return err
	})
	if err := eg.Wait(); err != nil {
		return nil, 0, err
	}
	return tasks, total, nil
}

func normalizeAndValidate(category *domain.TaskCategory) error {
	category.Name = strings.TrimSpace(category.Name)
	category.Description = strings.TrimSpace(category.Description)
	if category.Name == "" {
		return fmt.Errorf("category name cannot be empty")
	}
	if utf8.RuneCountInString(category.Name) > 128 {
		return fmt.Errorf("category name cannot exceed 128 characters")
	}
	if utf8.RuneCountInString(category.Description) > 500 {
		return fmt.Errorf("category description cannot exceed 500 characters")
	}
	return nil
}
