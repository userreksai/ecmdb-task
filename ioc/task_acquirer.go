package ioc

import (
	"github.com/userreksai/ecmdb-task/internal/repository"
	"github.com/userreksai/ecmdb-task/internal/service/acquirer"
)

func InitMySQLTaskAcquirer(taskRepo repository.TaskRepository) acquirer.TaskAcquirer {
	return acquirer.NewTaskAcquirer(taskRepo)
}
