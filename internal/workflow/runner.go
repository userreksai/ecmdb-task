package workflow

import (
	"context"
	"fmt"

	ecmdbrunnerv1 "github.com/userreksai/ecmdb-task/api/proto/gen/ecmdb/runner/v1"
)

// RunnerCatalog 执行单元目录：执行单元与脚本由 ECMDB 管理，这里只做查询与解析。
type RunnerCatalog interface {
	FindByID(ctx context.Context, id int64) (*ecmdbrunnerv1.Runner, error)
	ListByCodebookID(ctx context.Context, codebookID int64) ([]*ecmdbrunnerv1.Runner, error)
	ResolveExecution(ctx context.Context, runnerID int64) (RunnerExecution, error)
}

type ecmdbRunnerCatalog struct {
	client ecmdbrunnerv1.RunnerServiceClient
}

// NewRunnerCatalog 基于 ECMDB ecmdb.runner.v1 gRPC 服务创建执行单元目录。
func NewRunnerCatalog(client ecmdbrunnerv1.RunnerServiceClient) RunnerCatalog {
	return &ecmdbRunnerCatalog{client: client}
}

func (c *ecmdbRunnerCatalog) FindByID(ctx context.Context, id int64) (*ecmdbrunnerv1.Runner, error) {
	resp, err := c.client.FindRunnerByID(ctx, &ecmdbrunnerv1.FindRunnerByIDRequest{Id: id})
	if err != nil {
		return nil, err
	}
	if resp.GetRunner() == nil || resp.GetRunner().GetId() <= 0 {
		return nil, fmt.Errorf("执行单元 %d 不存在", id)
	}
	return resp.GetRunner(), nil
}

func (c *ecmdbRunnerCatalog) ListByCodebookID(ctx context.Context, codebookID int64) ([]*ecmdbrunnerv1.Runner, error) {
	resp, err := c.client.ListRunnersByCodebookID(ctx, &ecmdbrunnerv1.ListRunnersByCodebookIDRequest{CodebookId: codebookID})
	if err != nil {
		return nil, err
	}
	return resp.GetRunners(), nil
}

func (c *ecmdbRunnerCatalog) ResolveExecution(ctx context.Context, runnerID int64) (RunnerExecution, error) {
	resp, err := c.client.ResolveExecution(ctx, &ecmdbrunnerv1.ResolveExecutionRequest{RunnerId: runnerID})
	if err != nil {
		return RunnerExecution{}, err
	}
	exec := resp.GetExecution()
	if exec == nil || exec.GetRunner() == nil || exec.GetRunner().GetId() <= 0 {
		return RunnerExecution{}, fmt.Errorf("执行单元 %d 不存在", runnerID)
	}
	runner := exec.GetRunner()
	vars := make([]Variable, 0, len(exec.GetVariables()))
	for _, v := range exec.GetVariables() {
		vars = append(vars, Variable{Key: v.GetKey(), Value: v.GetValue(), Secret: v.GetSecret()})
	}
	return RunnerExecution{
		RunnerID: runner.GetId(), Name: runner.GetName(), CodebookID: runner.GetCodebookId(),
		Kind: runner.GetKind(), Target: runner.GetTarget(), Handler: runner.GetHandler(),
		Code: exec.GetCode(), Language: exec.GetLanguage(), Variables: vars,
	}, nil
}
