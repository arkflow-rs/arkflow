# Design: 分布式 rescale（Agent 恢复路径接入 key-group 重分布）

## Context

`RescaleContext`（`job_runner_adapter.rs:807-900`）已实现完整的重分布语义：命名空间解析算子 → 状态键白名单解码还原路由哈希输入 → `key_group_for_key` 归属 → 命名空间重写到新 owner task。本地 runner 在 `validate_manifest_task_compatibility` 失败且 `spec.rescale` 时启用（`job_runner_adapter.rs:107-119`），恢复时经 `restore_local_snapshot` 逐条重分布后合并进单一状态后端。

分布式路径的结构差异：状态物理上分属多节点，恢复是"每节点只恢复自己 assignment 的任务"。当前 `agent.rs:645-704` 的恢复分支按 assignment 过滤 manifest 快照引用后直接 restore，且入口校验（`validate_recovery_manifest`/`validate_recovery_snapshots`）要求任务集精确相等——任务集不同即 fail-closed，`RescaleContext` 从未被构造。

## Goals / Non-Goals

**Goals**：`rescale: true` 的分布式 Job 在 stop → 改 parallelism → start 后能从既有 savepoint/checkpoint 恢复；每个状态条目恰好被一个节点恢复；未声明 rescale 行为逐位不变；本地路径零回归。

**Non-Goals**：见 proposal。

## Decisions

### D1：兼容性评估加显式参数，而非字符串判别或复制函数

`evaluate_recovery_compatibility` 与 `validate_state_snapshot_task_set` 增加 `allow_task_set_mismatch: bool` 参数：

- false：逐位保持现有语义（本地路径调用点全部传 false）。
- true：仍执行校验和/Job 身份/格式/版本方向/**重复项**检查，仅跳过"manifest 任务集 == 计划任务集"的相等性断言。重复任务/快照引用依旧拒绝——重分布按 (namespace, key) 合并虽然幂等，重复引用意味着封存损坏，fail-closed 更诚实。

替代方案（解析错误字符串判断"是否恰好失败在任务集阶段"、或复制一对 `_for_rescale` 函数）分别引入脆弱耦合与双实现漂移，弃用。

### D2：Agent 侧重分布的读取范围与过滤时机

`spec.rescale` 时 Agent 读取 manifest 的**全部** `state_snapshots`（每条仍过命名空间前缀与格式校验），逐条 `RescaleContext::redistribute`，然后按"重分布后命名空间的 task 段 ∈ 本节点 assignment"过滤，合并恢复进本节点 redb。

- **为什么读全部**：key-group 重分布会把原属任务 A 的条目搬给任务 B；按旧 assignment 过滤会漏掉搬入条目。
- **为什么恢复前过滤**：不过滤则每个节点都恢复全量状态，写放大 N 倍且预算核算失真；过滤后全集群恰好一份。
- 过滤判定：`RescaleContext` 新增公开助手 `task_of_namespace(&str) -> Result<String>`（与 `namespace_operator` 对称，解析 `:task:` 段并反转百分号编码）。

### D3：复用而非重写 `RescaleContext`

将 `RescaleContext` 与 `redistribute` 从 `pub(crate)` 提升为 `arkflow_core::executor` 公开导出。Agent 不自己实现解码白名单——两处白名单必然漂移，且窗口/StatefulOperator 键编码属内核私有契约。

### D4：Hub 工件选择同步放宽

`recovery_record_is_valid`（agent.rs，Hub 恢复选择复用）在 `spec.rescale` 时传 `allow_task_set_mismatch=true`。这样 stop→改并行度→start 的调度链里，Hub 不会在"选择恢复工件"一步就把旧工件过滤光（否则 Agent 侧的放宽永远走不到）。身份/格式/版本方向仍校验，降级与跨格式依旧拒绝。

### D5：源位点与水位不动

`RecoveryPlan::from_manifest` 恢复的源位点按分区组织、由各节点 input 自行认领（`input.restore_positions`），与任务集无关；水位恢复同理。rescale 后 partition→subtask 归属可能变化，位点仍按分区精确恢复——这正是需要的语义，零改动。

## Risks / Trade-offs

- [重分布读取全量快照的带宽放大] → 只在任务集确实不一致时走该路径（一致时维持按 assignment 过滤的旧路径）；快照本身有对象存储局部性，一次性恢复成本可接受。
- [大状态恢复时间随规模线性] → 既有事实（全量 JSON 快照），rescale 不恶化语义，增量快照另行立项。
- [allow 参数被误用为绕过校验] → 参数仅在两处显式传入（Agent 恢复分支、Hub 工件选择），都以 `spec.rescale` 为前提；本地路径与既有测试全部传 false 保持回归。
- [新旧并行度的 max_parallelism 变化] → `key_group_for_key` 以 `max_parallelism` 划分；若用户同时改了 max_parallelism，key-group 归属会整体漂移，条目仍会一致地落到新 owner（不丢不重），但数据局部性变化——文档说明 rescale 语义以新声明的 max_parallelism 为准。

## Migration Plan

无 schema/存储变更。升级后：旧 Job（未声明 rescale）行为不变；声明 rescale 的 Job 立即获得分布式重分布能力。回滚 = 回退二进制。

## Open Questions

（无）
