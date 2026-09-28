## Why

节点故障时的重放置是"全量重算"而非"增量迁移"，且同代恢复路径存在死锁洞：

1. **映射全量漂移**：`hub/placement.rs` 的 `reconcile_job` 在任一原节点离线（`retention_won` 失败）后，对整个 ranked 目标集重跑 `assignments_for_nodes`（`job.rs:1081-1105` 的 `index % len`）——节点数变化使**每个** task 的归属重排，存活节点上仍健康的 subtask 的 task→node 映射全部改变。
2. **同代 start 跳过使重放置无法生效**：派发循环以"Succeeded start at (node, job, generation)"为终态记忆直接跳过（`placement.rs:655-669`），存活节点根本不会收到新映射；死节点的 task 无人接管。
3. **崩溃内核无复活路径**：内核失败只更新 job 观测（`lifecycle.rs:6-75` `observe_job`），该节点的 Succeeded start 仍满足跳过条件——除非运维显式 restart（bump generation），死掉的 task 永不重启。远程边 fail-closed（split 放置下节点故障必然触发）之后系统就停在这里。

## What Changes

- **增量重放置**：自动放置的 job_start 在部分节点离线/被驱逐时，保持既有派发顺序，仅在原位替换失效槽位（placement_order 记忆；缺失时退化为排序的 previous_nodes）。存活节点的 assignment 集逐位不变——其 Succeeded start 与运行中内核保持真实。无新候选时可复制存活节点占据槽位（`assignments_for_nodes` 的 round-robin 允许重复节点）；无存活节点时维持今天的全量 ranked 重放置。
- **派发指纹门**：Hub 在派发 start 时记录每节点的 assignment 任务集指纹（内存）；跳过条件从"有 Succeeded start"收紧为"有 Succeeded start 且指纹与当前计算一致"。指纹失配的 Succeeded start 被 superseded 并重新派发。Hub 重启后指纹内存清空 → 存活节点各收到一次 start：assignment 一致的 Agent 端幂等 no-op，漂移的替换内核（自愈，见下）。
- **失败观察失效 start**：节点对某 job 上报 failed 观测时，该节点同代 Succeeded start 被置为可重试终态（TimedOut + failure_class `runtime_failed`），重新进入派发循环——崩溃内核（如 split 远程边 fail-closed）不再需要运维重启才恢复。
- **Agent assignment 漂移替换**：同代 start 命令携带的 per-node 任务集与运行中内核记录一致时保持幂等 no-op；不一致时走既有拆除+恢复路径重建内核（JobTask 记录其 assignment 任务集）。
- 诚实边界：split 放置下存活节点与死节点的远程边断开后按既有协议 fail-closed（重连预算内透明恢复，预算外内核失败→经上述失败路径以稳定映射重启）——本变更不实现"不停机的边重定向"；其价值是**映射稳定 + 自动收敛**：故障窗口 = 边失败发现 + checkpoint 恢复，且存活节点的 task 集不再漂移、colocated（默认）放置的存活节点完全不动。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `distributed-job-runtime`: "Job lifecycle SHALL support recovery operations" 需求扩展——部分节点故障时的增量重放置语义、失败观察的重派发、同代 start 的 assignment 一致性语义。

## Impact

- `crates/arkflow-server/src/hub/placement.rs`：增量槽位替换、派发指纹、失败观察失效。
- `crates/arkflow-server/src/hub.rs`：指纹内存字段。
- `crates/arkflow-server/src/agent.rs`：JobTask 记录 assignment 集；同代 start 漂移替换。
- `crates/arkflow-server/src/hub/tests.rs` + agent tests：节点丢失映射稳定性、指纹失配重派发、失败复活、Agent 漂移替换。
- 文档（en + zh-Hans）：部分故障行为与恢复窗口说明。

## Non-goals

- 不做运行中远程边的热重定向（存活内核不停机换目标）——split 存活节点在边断开时仍按既有 fail-closed/重连协议处理。
- 不做 per-task 独立 generation 或 task attempt 的持久化追踪（`cp_job_tasks` 激活另行立项）。
- 不改 checkpoint 协议与恢复语义（接管节点仍从最近 completed checkpoint 恢复）。
- 不覆盖 pinned `node_ids` 放置（显式固定节点集维持现状语义）。
