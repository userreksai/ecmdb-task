package category

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/userreksai/ecmdb-task/internal/domain"
)

type fakeCategoryRepository struct {
	createdCategory domain.TaskCategory
	replacedID      int64
	replacedBizID   int64
	replacedTaskIDs []int64
}

func (f *fakeCategoryRepository) Create(_ context.Context, category domain.TaskCategory) (int64, error) {
	f.createdCategory = category
	return 11, nil
}

func (f *fakeCategoryRepository) FindByID(context.Context, int64) (domain.TaskCategory, error) {
	return domain.TaskCategory{}, nil
}

func (f *fakeCategoryRepository) List(context.Context, int64, string) ([]domain.TaskCategory, error) {
	return nil, nil
}

func (f *fakeCategoryRepository) CountUncategorized(context.Context, int64) (int64, error) {
	return 0, nil
}

func (f *fakeCategoryRepository) Update(context.Context, domain.TaskCategory) (int64, error) {
	return 1, nil
}

func (f *fakeCategoryRepository) Delete(context.Context, int64) (int64, error) {
	return 1, nil
}

func (f *fakeCategoryRepository) ReplaceTasks(_ context.Context, categoryID, bizID int64, taskIDs []int64) error {
	f.replacedID = categoryID
	f.replacedBizID = bizID
	f.replacedTaskIDs = taskIDs
	return nil
}

func (f *fakeCategoryRepository) ListTasks(
	context.Context,
	int64,
	*int64,
	string,
	int,
	int,
) ([]domain.Task, error) {
	return nil, nil
}

func (f *fakeCategoryRepository) CountTasks(context.Context, int64, *int64, string) (int64, error) {
	return 0, nil
}

func TestServiceCreateNormalizesCategory(t *testing.T) {
	repo := &fakeCategoryRepository{}
	svc := NewService(repo)

	id, err := svc.Create(context.Background(), domain.TaskCategory{
		Name:        "  商城项目  ",
		Description: "  核心交易任务  ",
	})

	require.NoError(t, err)
	require.Equal(t, int64(11), id)
	require.Equal(t, "商城项目", repo.createdCategory.Name)
	require.Equal(t, "核心交易任务", repo.createdCategory.Description)
}

func TestServiceReplaceTasksDeduplicatesIDs(t *testing.T) {
	repo := &fakeCategoryRepository{}
	svc := NewService(repo)

	err := svc.ReplaceTasks(context.Background(), 7, []int64{3, 3, 5})

	require.NoError(t, err)
	require.Equal(t, int64(7), repo.replacedID)
	require.NotZero(t, repo.replacedBizID)
	require.Equal(t, []int64{3, 5}, repo.replacedTaskIDs)
}

func TestServiceRejectsInvalidCategoryAndTaskIDs(t *testing.T) {
	svc := NewService(&fakeCategoryRepository{})

	_, err := svc.Create(context.Background(), domain.TaskCategory{Name: "   "})
	require.Error(t, err)
	require.Error(t, svc.ReplaceTasks(context.Background(), 0, []int64{1}))
	require.Error(t, svc.ReplaceTasks(context.Background(), 1, []int64{0}))
}
