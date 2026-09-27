# Proposal: add-job-rescale-guard

## Why

并行度变更后的 checkpoint 恢复存在**静默状态失联**隐患（PLANNING.md 第八节 P2「add-job-rescale」调研结论）：keyed 状态命名空间内嵌 task id（`job:…:task:op-N`），并行度变化改变任务集与 key-group 归属，旧命名空间下的状态在恢复后无人读取，而源位点照常恢复——作业以空状态继续，产生无告警的数据缺口。真正的 key 重分布需要逐算子状态键解码（窗口键内嵌 window_start，计数器另有编码），是独立的大工程；本变更先交付正确性守卫：把静默失联变成显式失败。

## What Changes

1. 本地 Job 恢复路径新增 `validate_manifest_task_compatibility`：恢复工件的 `task_attempts` 任务集与当前 plan 任务集不一致（并行度或算子拓扑变更）即 fail-closed，错误信息给出两条出路（恢复原并行度 / 用新 checkpoint/savepoint 重置状态）。
2. 并行度保持不变时零行为变化；无状态作业变更并行度不受影响（无恢复工件可比对）。
3. key 重分布（真正的在线 rescale）明确为后续 change；规格记录边界。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `checkpoint-recovery`: 恢复工件与当前计划的任务集兼容性成为恢复前置校验；不兼容即显式失败。

## Impact

- `crates/arkflow-core/src/executor/job_runner_adapter.rs`：恢复路径守卫 + 单测。
- 面向用户的错误信息变更（并行度变更后恢复原为静默失联，现在显式失败）。

## Non-goals

key 状态重分布（savepoint → 变更并行度 → 按 key-group 重映射恢复）；在线（不停顿）rescale；Agent 分布式部署侧的同型守卫（后续跟随 Hub 恢复编排）。
