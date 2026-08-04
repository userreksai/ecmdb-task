package ioc

import (
	"github.com/userreksai/ecmdb-task/internal/repository"
	"github.com/userreksai/ecmdb-task/internal/service/picker"
	"github.com/userreksai/ecmdb-task/pkg/grpc/registry"
)

// InitExecutorNodePicker 初始化节点选择器（只负责选节点）
func InitExecutorNodePicker(reg registry.Registry) picker.ExecutorNodePicker {
	return picker.NewRandomPicker(reg)
}

// InitExecModeResolver 初始化模式感知器（负责查 metadata + 写 tasks.exec_mode）
func InitExecModeResolver(reg registry.Registry, taskRepo repository.TaskRepository) picker.IExecModeResolver {
	return picker.NewExecModeResolver(reg, taskRepo)
}
