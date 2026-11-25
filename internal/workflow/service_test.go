package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	ecmdbrunnerv1 "github.com/userreksai/ecmdb-task/api/proto/gen/ecmdb/runner/v1"
	"github.com/userreksai/ecmdb-task/internal/domain"
	"github.com/userreksai/ecmdb-task/internal/errs"
	"github.com/userreksai/ecmdb-task/internal/event"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ---------- fakes ----------

type memDAO struct {
	mu   sync.Mutex
	rows []Request
}

func (m *memDAO) FindOrCreate(_ context.Context, r Request) (Request, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, row := range m.rows {
		if row.RequestID == r.RequestID {
			return row, false, nil
		}
	}
	r.ID = int64(len(m.rows) + 1)
	m.rows = append(m.rows, r)
	return r, true, nil
}

func (m *memDAO) find(pred func(Request) bool) (Request, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, row := range m.rows {
		if pred(row) {
			return row, nil
		}
	}
	return Request{}, ErrRequestNotFound
}

func (m *memDAO) FindByRequestID(_ context.Context, id string) (Request, error) {
	return m.find(func(r Request) bool { return r.RequestID == id })
}
func (m *memDAO) FindByTaskID(_ context.Context, id int64) (Request, error) {
	return m.find(func(r Request) bool { return r.TaskID == id })
}
func (m *memDAO) FindByExecutionID(_ context.Context, id int64) (Request, error) {
	return m.find(func(r Request) bool { return r.ExecutionID == id })
}
func (m *memDAO) set(id int64, fn func(*Request)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.rows {
		if m.rows[i].ID == id {
			fn(&m.rows[i])
			return nil
		}
	}
	return ErrRequestNotFound
}
func (m *memDAO) BindTask(_ context.Context, id, taskID int64) error {
	return m.set(id, func(r *Request) { r.TaskID = taskID })
}
func (m *memDAO) BindExecution(_ context.Context, id, execID int64) error {
	return m.set(id, func(r *Request) { r.ExecutionID = execID })
}
func (m *memDAO) MarkCancel(_ context.Context, id int64, reason string) error {
	return m.set(id, func(r *Request) { r.CancelRequested, r.CancelReason = true, reason })
}

type fakeCatalog struct{ kind string }

func (f fakeCatalog) FindByID(context.Context, int64) (*ecmdbrunnerv1.Runner, error) { return nil, nil }
func (f fakeCatalog) ListByCodebookID(context.Context, int64) ([]*ecmdbrunnerv1.Runner, error) {
	return nil, nil
}
func (f fakeCatalog) ResolveExecution(_ context.Context, id int64) (RunnerExecution, error) {
	return RunnerExecution{RunnerID: id, Name: "deploy", Kind: f.kind, Target: "execute", Handler: "shell",
		Code: "echo hi", Language: "shell",
		Variables: []Variable{{Key: "TOKEN", Value: "s3cr3t", Secret: true}, {Key: "ENV", Value: "prod"}}}, nil
}

// fakeEngine 同时扮演任务服务、执行服务、调度器与中断器
type fakeEngine struct {
	mu          sync.Mutex
	tasks       map[int64]domain.Task
	executions  map[int64]domain.TaskExecution
	stopped     []int64
	interrupted []int64
	states      []domain.ExecutionState
	scheduleErr error
	noExecution bool
}

func newFakeEngine() *fakeEngine {
	return &fakeEngine{tasks: map[int64]domain.Task{}, executions: map[int64]domain.TaskExecution{}}
}

func (f *fakeEngine) Create(_ context.Context, t domain.Task) (domain.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t.ID = int64(100 + len(f.tasks))
	f.tasks[t.ID] = t
	return t, nil
}
func (f *fakeEngine) GetByID(_ context.Context, id int64) (domain.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tasks[id], nil
}
func (f *fakeEngine) Stop(_ context.Context, id int64) error {
	f.stopped = append(f.stopped, id)
	return nil
}
func (f *fakeEngine) ScheduleNow(_ context.Context, t domain.Task) error {
	if f.noExecution {
		return f.scheduleErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	id := int64(1000 + len(f.executions))
	f.executions[id] = domain.TaskExecution{ID: id, Task: t, Status: domain.TaskExecutionStatusPrepare}
	return f.scheduleErr
}
func (f *fakeEngine) FindByID(_ context.Context, id int64) (domain.TaskExecution, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.executions[id], nil
}
func (f *fakeEngine) ListByTaskID(_ context.Context, taskID int64, _, _ int) ([]domain.TaskExecution, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, e := range f.executions {
		if e.Task.ID == taskID {
			return []domain.TaskExecution{e}, 1, nil
		}
	}
	return nil, 0, nil
}
func (f *fakeEngine) UpdateState(_ context.Context, s domain.ExecutionState) error {
	f.states = append(f.states, s)
	return nil
}
func (f *fakeEngine) Interrupt(_ context.Context, e domain.TaskExecution) error {
	f.interrupted = append(f.interrupted, e.ID)
	return nil
}

func newTestService(kind string, eng *fakeEngine) (*service, *memDAO) {
	dao := &memDAO{}
	svc := NewService(dao, fakeCatalog{kind: kind}, eng, eng, eng, eng).(*service)
	svc.waitFor = 200 * time.Millisecond
	return svc, dao
}

// ---------- tests ----------

func TestRunRunner(t *testing.T) {
	testCases := []struct {
		name        string
		kind        string
		req         RunRequest
		scheduleErr error
		noExecution bool
		wantErr     error
		wantCode    codes.Code
	}{
		{name: "缺少 request_id", kind: RunnerKindGRPC, req: RunRequest{RunnerID: 1},
			wantErr: ErrInvalidRequest, wantCode: codes.InvalidArgument},
		{name: "KAFKA 执行单元暂不支持", kind: RunnerKindKafka, req: RunRequest{RequestID: "r1", RunnerID: 1},
			wantErr: ErrRejected, wantCode: codes.FailedPrecondition},
		{name: "提交成功", kind: RunnerKindGRPC,
			req: RunRequest{RequestID: "r1", RunnerID: 7, Params: map[string]string{"args": `{"a":1}`},
				Variables: map[string]string{"ENV": "test"}, MaxExecutionSeconds: 60}},
		{name: "调度循环抢先抢占时仍返回执行记录", kind: RunnerKindGRPC,
			req: RunRequest{RequestID: "r1", RunnerID: 7}, scheduleErr: errs.ErrTaskPreemptFailed},
		{name: "执行记录未生成时返回可重试错误", kind: RunnerKindGRPC,
			req: RunRequest{RequestID: "r1", RunnerID: 7}, noExecution: true,
			wantErr: ErrExecutionPending, wantCode: codes.Unavailable},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			eng := newFakeEngine()
			eng.scheduleErr, eng.noExecution = tc.scheduleErr, tc.noExecution
			svc, dao := newTestService(tc.kind, eng)

			res, err := svc.RunRunner(context.Background(), tc.req)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				assert.Equal(t, tc.wantCode, status.Code(toStatus(err)))
				return
			}
			require.NoError(t, err)
			assert.True(t, res.Created)
			assert.Positive(t, res.ExecutionID)

			row, _ := dao.FindByRequestID(context.Background(), tc.req.RequestID)
			assert.Equal(t, res.ExecutionID, row.ExecutionID)
			task := eng.tasks[row.TaskID]
			assert.Equal(t, domain.TaskTypeOneTime, task.Type)
			assert.Equal(t, "eflow-"+tc.req.RequestID, task.Name)
			assert.Equal(t, "execute", task.GrpcConfig.ServiceName)
			assert.Equal(t, "echo hi", task.GrpcConfig.Params["code"])
			assert.Equal(t, int32(0), task.RetryConfig.MaxRetries, "工作流执行不做执行器层重试")

			var vars []Variable
			require.NoError(t, json.Unmarshal([]byte(task.GrpcConfig.Params["variables"]), &vars))
			got := map[string]Variable{}
			for _, v := range vars {
				got[v.Key] = v
			}
			assert.True(t, got["TOKEN"].Secret, "执行单元敏感变量应保留")
			if tc.req.Variables != nil {
				assert.Equal(t, "test", got["ENV"].Value, "请求变量覆盖执行单元同名变量")
				assert.Equal(t, `{"a":1}`, task.GrpcConfig.Params["args"])
			}

			// 同一 request_id 再次提交：幂等返回同一执行，不再创建任务
			again, err := svc.RunRunner(context.Background(), tc.req)
			require.NoError(t, err)
			assert.False(t, again.Created)
			assert.Equal(t, res.ExecutionID, again.ExecutionID)
			assert.Len(t, eng.tasks, 1)
		})
	}
}

func TestTerminateExecution(t *testing.T) {
	ctx := context.Background()

	t.Run("提交未到达前终止：落终止标记并拒绝后续补发", func(t *testing.T) {
		svc, dao := newTestService(RunnerKindGRPC, newFakeEngine())
		require.NoError(t, svc.TerminateExecution(ctx, 0, "r-early", "人工终止"))
		row, err := dao.FindByRequestID(ctx, "r-early")
		require.NoError(t, err)
		assert.True(t, row.CancelRequested)

		_, err = svc.RunRunner(ctx, RunRequest{RequestID: "r-early", RunnerID: 7})
		assert.ErrorIs(t, err, ErrRejected)
	})

	testCases := []struct {
		name          string
		status        domain.TaskExecutionStatus
		wantInterrupt bool
		wantState     bool
	}{
		{name: "运行中：中断执行器", status: domain.TaskExecutionStatusRunning, wantInterrupt: true},
		{name: "尚未开始：直接终结执行", status: domain.TaskExecutionStatusPrepare, wantState: true},
		{name: "已终态：幂等返回", status: domain.TaskExecutionStatusSuccess},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			eng := newFakeEngine()
			svc, dao := newTestService(RunnerKindGRPC, eng)
			res, err := svc.RunRunner(ctx, RunRequest{RequestID: "r1", RunnerID: 7})
			require.NoError(t, err)
			e := eng.executions[res.ExecutionID]
			e.Status = tc.status
			eng.executions[res.ExecutionID] = e

			require.NoError(t, svc.TerminateExecution(ctx, res.ExecutionID, "", "人工终止"))
			row, _ := dao.FindByRequestID(ctx, "r1")
			assert.True(t, row.CancelRequested)
			assert.Equal(t, tc.wantInterrupt, len(eng.interrupted) == 1)
			assert.Equal(t, tc.wantState, len(eng.states) == 1)
			if tc.wantState {
				assert.Equal(t, domain.TaskExecutionStatusFailed, eng.states[0].Status)
			}
		})
	}
}

func TestBridgeTranslate(t *testing.T) {
	ctx := context.Background()
	dao := &memDAO{}
	wf, _, _ := dao.FindOrCreate(ctx, Request{RequestID: "wf-1", Source: SourceWorkflow})
	_ = dao.BindTask(ctx, wf.ID, 11)
	cancelled, _, _ := dao.FindOrCreate(ctx, Request{RequestID: "wf-2", Source: SourceWorkflow})
	_ = dao.BindTask(ctx, cancelled.ID, 12)
	_ = dao.MarkCancel(ctx, cancelled.ID, "stop")
	notify, _, _ := dao.FindOrCreate(ctx, Request{RequestID: "notify:1", Source: SourceOf("notify:1")})
	_ = dao.BindTask(ctx, notify.ID, 13)

	bridge := &Bridge{svc: NewService(dao, nil, nil, nil, nil, nil)}

	testCases := []struct {
		name       string
		evt        event.Event
		wantOK     bool
		wantStatus string
		wantReq    string
	}{
		{name: "成功", evt: event.Event{TaskID: 11, ExecID: 1, ExecStatus: domain.TaskExecutionStatusSuccess},
			wantOK: true, wantStatus: "SUCCESS", wantReq: "wf-1"},
		{name: "失败", evt: event.Event{TaskID: 11, ExecID: 1, ExecStatus: domain.TaskExecutionStatusFailed},
			wantOK: true, wantStatus: "FAILED", wantReq: "wf-1"},
		{name: "被终止的失败呈现为 CANCELLED", evt: event.Event{TaskID: 12, ExecStatus: domain.TaskExecutionStatusFailed},
			wantOK: true, wantStatus: "CANCELLED", wantReq: "wf-2"},
		{name: "通知脚本不回传", evt: event.Event{TaskID: 13, ExecStatus: domain.TaskExecutionStatusSuccess}},
		{name: "普通任务不回传", evt: event.Event{TaskID: 99, ExecStatus: domain.TaskExecutionStatusSuccess}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			out, ok, err := bridge.Translate(ctx, tc.evt)
			require.NoError(t, err)
			assert.Equal(t, tc.wantOK, ok)
			if ok {
				assert.Equal(t, tc.wantStatus, out.ExecStatus)
				assert.Equal(t, tc.wantReq, out.RequestID)
				assert.Equal(t, "WORKFLOW", out.Source)
			}
		})
	}
}

func TestToStatus(t *testing.T) {
	assert.Equal(t, codes.Internal, status.Code(toStatus(errors.New("db down"))))
}
