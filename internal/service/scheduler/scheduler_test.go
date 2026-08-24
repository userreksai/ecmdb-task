package scheduler

import (
	"context"
	"errors"
	"testing"

	"github.com/gotomicro/ego/core/elog"
	"github.com/userreksai/ecmdb-task/internal/domain"
)

type fakeScheduleTaskService struct {
	updatedIDs []int64
	updateErr  error
}

func (f *fakeScheduleTaskService) SchedulableTasks(context.Context, int64, int) ([]domain.Task, error) {
	return nil, nil
}

func (f *fakeScheduleTaskService) UpdateNextTime(_ context.Context, id int64) (domain.Task, error) {
	f.updatedIDs = append(f.updatedIDs, id)
	if f.updateErr != nil {
		return domain.Task{}, f.updateErr
	}
	return domain.Task{ID: id, NextTime: 2000}, nil
}

func TestSchedulerSkipMissedRecurringTask(t *testing.T) {
	testCases := []struct {
		name        string
		task        domain.Task
		wantSkipped bool
		wantUpdates int
	}{
		{
			name: "skip recurring task due before startup",
			task: domain.Task{
				ID:       1,
				Type:     domain.TaskTypeRecurring,
				NextTime: 999,
			},
			wantSkipped: true,
			wantUpdates: 1,
		},
		{
			name: "run recurring task due at startup",
			task: domain.Task{
				ID:       2,
				Type:     domain.TaskTypeRecurring,
				NextTime: 1000,
			},
		},
		{
			name: "run recurring task due after startup",
			task: domain.Task{
				ID:       3,
				Type:     domain.TaskTypeRecurring,
				NextTime: 1001,
			},
		},
		{
			name: "do not change overdue one-time task",
			task: domain.Task{
				ID:       4,
				Type:     domain.TaskTypeOneTime,
				NextTime: 999,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			taskSvc := &fakeScheduleTaskService{}
			s := &Scheduler{
				taskSvc:   taskSvc,
				startedAt: 1000,
				ctx:       context.Background(),
				logger:    elog.DefaultLogger.With(elog.FieldComponentName("SchedulerTest")),
			}

			skipped, err := s.skipMissedRecurringTask(tc.task)
			if err != nil {
				t.Fatalf("skipMissedRecurringTask() error = %v", err)
			}
			if skipped != tc.wantSkipped {
				t.Fatalf("skipMissedRecurringTask() skipped = %v, want %v", skipped, tc.wantSkipped)
			}
			if len(taskSvc.updatedIDs) != tc.wantUpdates {
				t.Fatalf("UpdateNextTime() calls = %d, want %d", len(taskSvc.updatedIDs), tc.wantUpdates)
			}
		})
	}
}

func TestSchedulerDoesNotDispatchMissedRecurringTaskWhenAdvanceFails(t *testing.T) {
	wantErr := errors.New("database unavailable")
	taskSvc := &fakeScheduleTaskService{updateErr: wantErr}
	s := &Scheduler{
		taskSvc:   taskSvc,
		startedAt: 1000,
		ctx:       context.Background(),
		logger:    elog.DefaultLogger.With(elog.FieldComponentName("SchedulerTest")),
	}

	skipped, err := s.skipMissedRecurringTask(domain.Task{
		ID:       1,
		Type:     domain.TaskTypeRecurring,
		NextTime: 999,
	})
	if !skipped {
		t.Fatal("skipMissedRecurringTask() skipped = false, want true")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("skipMissedRecurringTask() error = %v, want wrapped %v", err, wantErr)
	}
}
