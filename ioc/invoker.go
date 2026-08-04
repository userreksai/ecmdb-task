package ioc

import (
	executorv1 "github.com/userreksai/ecmdb-task/api/proto/gen/etask/executor/v1"
	"github.com/userreksai/ecmdb-task/internal/service/invoker"
	"github.com/userreksai/ecmdb-task/pkg/grpc/pool"
)

func InitInvoker(clients *pool.Clients[executorv1.ExecutorServiceClient]) invoker.Invoker {
	return invoker.NewDispatcher(
		invoker.NewHTTPInvoker(),
		invoker.NewGRPCInvoker(clients),
		invoker.NewLocalInvoker(map[string]invoker.LocalExecuteFunc{}, map[string]invoker.LocalPrepareFunc{}))
}
