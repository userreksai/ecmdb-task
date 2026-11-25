# eflow 工作流执行接入

eflow（工单流程编排）的自动化节点与通知脚本通过上游 etask 新版协议提交执行。ecmdb-task 在**不改动原有任务管理、
分类、SSE、Agent、补偿逻辑**的前提下，新增 `internal/workflow` 模块做协议适配。

## 提供的接口（调度中心 gRPC，服务名 `grpc.server.scheduler.name`）

| 接口 | 说明 |
|---|---|
| `etask.scheduler.v1.SchedulerService/RunRunner` | 以 `request_id` 幂等提交：按 `runner_id` 从 ECMDB 解析脚本与变量，创建一次性任务并立即调度，返回执行 ID |
| `etask.scheduler.v1.SchedulerService/TerminateExecution` | 按执行 ID 或 `request_id` 终止；执行未创建时先持久化终止意图，之后同一 `request_id` 的补发被拒绝 |
| `etask.runner.v1.RunnerService/FindRunnerByID`、`ListRunnersByCodebookID` | 执行单元查询，代理到 ECMDB，不返回脚本与变量 |
| `etask.executor.v1.TaskExecutionService/GetTaskExecution` | 新增：按执行 ID 查询（eflow 对账使用）；被终止的执行以 `CANCELLED` 呈现。原有接口行为不变 |

错误码约定：`InvalidArgument` / `FailedPrecondition` 表示拒绝（eflow 不重试）；其它错误（含 `Unavailable`）
表示可重试，eflow 用同一 `request_id` 幂等重投。

## 执行链路

```
RunRunner ─► workflow_requests（request_id 幂等）
          ─► ECMDB ecmdb.runner.v1.ResolveExecution（脚本、语言、解密后的变量）
          ─► tasks（ONE_TIME，name = eflow-<request_id>，不做执行器层重试）
          ─► Scheduler.ScheduleNow（与调度循环同一条选节点/抢占/执行路径，乐观锁保证只执行一次）
          ─► task_executions ─► 执行器 ─► complete_topic
                                              └─► 独立消费组 eflow-bridge ─► task_execution_complete_events（eflow）
```

- 执行器参数与 ECMDB 原有下发格式一致：`code`、`args`、`variables`（另附 `request_id`）。
- 进程内调度节点 ID 为单例：gRPC 服务端立即抢占的任务由调度循环统一续约，长任务不会被误判为僵尸任务。
- 通知脚本（`request_id` 以 `notify:` 开头）的终态不回传 eflow。
- 目前仅支持 **GRPC 类型**的执行单元；KAFKA 类型（Kafka Agent）提交会被拒绝。

## 依赖

- ECMDB 需提供 `ecmdb.runner.v1.RunnerService`（proto 见 `api/proto/ecmdb/runner/v1/runner.proto`），
  使用现有配置 `grpc.client.ecmdb` 连接。
- 新表 `workflow_requests` 启动时自动创建（AutoMigrate）。
