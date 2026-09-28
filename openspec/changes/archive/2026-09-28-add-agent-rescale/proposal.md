## Why

`JobSpec.rescale: true` 的 key-group 状态重分布只在本地 runner 路径实现（`crates/arkflow-core/src/executor/job_runner_adapter.rs:107-119` 构造 `RescaleContext`，`restore_local_snapshot` 在恢复时重写命名空间）；分布式 Agent 路径在恢复前强制"manifest 任务集与当前计划完全一致"——`agent.rs:242-279` 的 `validate_recovery_manifest` 调用 `evaluate_recovery_compatibility`，`agent.rs:281-304` 的 `validate_recovery_snapshots` 调用 `validate_state_snapshot_task_set`，两者对任务集差异一律拒绝；Hub 侧工件选择 `recovery_record_is_valid`（`agent.rs:306`，被 hub 引用）同样拒绝。结果：分布式 Job 变更并行度只能 fail-closed（换新 checkpoint 重置状态），checkpoint-recovery spec 已承诺的重分布语义在部署形态上不成立。

## What Changes

- `arkflow-core::checkpoint` 的两个兼容性评估函数增加"允许任务集差异"的显式参数（身份/校验和/格式/版本方向/重复项检查保持逐位不变；仅任务集相等性按参数跳过），本地路径行为不变。
- 导出 `RescaleContext`（含按命名空间解析目标任务的公开助手），供 Agent 恢复路径复用同一重分布实现，不引入第二套解码白名单。
- Agent 恢复路径（`agent.rs` spawn_kernel_job 的恢复分支）：`spec.rescale` 且任务集不一致时，读取 manifest 的**全部**状态快照（而非仅本节点 assignment），逐条重分布到新任务命名空间，然后**只保留新命名空间属于本节点 assignment 的条目**恢复进本节点状态后端——每个条目恰好落一个节点，无重复恢复。
- Hub 侧工件选择（`recovery_record_is_valid`）在 `spec.rescale` 时同样放宽任务集检查，使既有 savepoint/checkpoint 在变更并行度后仍可被选为恢复工件。
- 未声明 rescale 的分布式恢复保持现有 fail-closed 守卫与错误文案不变。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `checkpoint-recovery`: 任务集兼容性需求与 rescale 重分布需求扩展到分布式 Agent 恢复路径（含按节点过滤语义与 Hub 工件选择放宽）。

## Impact

- `crates/arkflow-core/src/checkpoint.rs`：兼容性评估参数化。
- `crates/arkflow-core/src/executor/job_runner_adapter.rs`：`RescaleContext` 可见性与导出。
- `crates/arkflow-server/src/agent.rs`：恢复分支接入重分布与按节点过滤；`recovery_record_is_valid` 放宽。
- 测试：核心侧重分布单元测试已有（本地路径）；新增 Agent 路径的恢复重分布测试（内存 checkpoint store + 两并行度计划 + 单节点 assignment 过滤断言）。
- 文档：`docs/docs/` 与 zh-Hans 中 Job rescale/分布式部署页说明分布式路径已支持。

## Non-goals

- 不做运行中（不停机）rescale：仍要求 stop → 变更 parallelism → start。
- 不支持算子拓扑变更（新增/删除状态算子）的重分布——白名单解码与 owner 解析仍要求原有算子齐备，违反时显式失败（与本地路径同语义）。
- 不改 checkpoint 封存/聚合协议（两阶段全局 checkpoint 不变；恢复后的下一个 checkpoint 自然按新任务集封存）。
- 不改源位点/水位恢复语义（按分区恢复与任务归属无关，沿用现状）。
