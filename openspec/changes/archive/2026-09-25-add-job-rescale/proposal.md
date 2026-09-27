# Proposal: add-job-rescale

## Why

前序变更 `add-job-rescale-guard` 把"并行度变更后恢复 = 静默状态失联"变成了显式失败，并把真 key 重分布记录为后续（PLANNING.md 第八节 P2 的原计划项：savepoint → 变更 parallelism → key-group 重映射 + 状态重分布）。本变更交付重分布本体。

## What Changes

1. `JobSpec.rescale: bool`（serde default false）：显式声明"允许跨任务集恢复时重分布 keyed 状态"。未声明时既有守卫行为不变（fail-closed + 可操作错误）。
2. 恢复路径：守卫检测到任务集不匹配且 `rescale: true` 时，构造 `RescaleContext` 交给 `restore_local_snapshot` 执行重分布而非失败。
3. **重分布算法**：快照条目 (namespace, key) → 从 namespace 解析 operator id → 按算子类型从状态键还原**路由哈希输入**：
   - 窗口算子：跳过 8 字节 window_start，剩余 utf8 即路由输入（与 `hash_column` 的 String 路径一致）；
   - StatefulOperator：剥 `"<tag>:"` 前缀，剩余字节即路由输入（整数 BE / utf8 / binary 与 `hash_column` 逐字节一致）；`null:<tag>` 哨兵整条即输入。
   → `key_group_for_key(input, max_parallelism)` → 新 plan 中拥有该 key-group 的任务 → 条目 namespace 重写为该任务的命名空间（value/key 原样保留）。
4. 无法解码的键编码 → 显式失败（fail-closed，不猜）。
5. 源位点：按 (input, partition) 各自恢复，天然适配新任务-分区分工；新增分区的起点由输入自身策略决定（文档明示）。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `checkpoint-recovery`: 「任务集兼容性守卫」扩展——`rescale: true` 显式声明时按 key-group 重分布而非失败；新增重分布语义需求（路由一致性、编码白名单、不可解码 fail-closed）。

## Impact

- `crates/arkflow-core/src/job.rs`：`JobSpec.rescale` 字段。
- `crates/arkflow-core/src/executor/job_runner_adapter.rs`：`RescaleContext`（routing_key_bytes/redistribute）+ `restore_local_snapshot` 重分布参数 + 守卫分支。
- 测试：stateful 键重分布、窗口键重分布（剥 window_start）、未知编码拒绝、守卫不变性；core 509 绿。

## Non-goals

在线（不停顿）rescale；Agent/Hub 分布式部署侧编排（本地恢复路径先行，Hub 侧跟随恢复编排单独立项）；跨 max_parallelism 变更（key-group 数变化时归属只能近似，显式不支持——文档明示 max_parallelism 必须不变）。
