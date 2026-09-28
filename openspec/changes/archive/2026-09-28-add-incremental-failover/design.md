# Design: 增量重放置与故障重派发

## Context

放置管线：`reconcile_job`（`hub/placement.rs:224`）→ 候选/ranked targets → 保留判定（`retention_won`）→ `assignments_for_nodes`（round-robin `index % len`）→ 终态跳过派发。派发顺序记忆 `placement_order`（内存）。Agent 端同代活内核对重投 start 幂等 no-op（`agent.rs:627-640`）。

三个耦合问题（见 proposal Why）：映射全量漂移、同代跳过阻断裂放置、崩溃内核无重派发路径。任何单点修补都不闭合——映射稳定必须配指纹门（否则 Hub 重启后跳过仍吞掉新映射），指纹门必须配 Agent 漂移替换（否则重派发的 start 被 no-op 吞掉），split 的边 fail-closed 必须配失败重派发（否则内核失败后 Succeeded start 仍跳过）。

## Goals / Non-Goals

**Goals**：部分节点故障时（自动放置）存活节点 assignment 逐位稳定；接管节点自动获得失效槽位的 task；崩溃内核在同代内自动重派发；Hub 重启后的映射漂移可自愈。

**Non-Goals**：见 proposal。

## Decisions

### D1：槽位原位替换（不是重算映射）

`index % len` 的分配语义使"有序列表的原位替换"天然保持其他槽位不变：把失效槽位（离线/驱逐/无 shuffle 能力）替换为 ranked 候选，或在无候选时复制最优存活节点（round-robin 允许重复节点 id；side-edge 校验只拒绝重复 task，不拒绝重复节点）。存活节点因此保持精确相同的 task 集——其 Succeeded start 与内核继续保持真实，跳过逻辑成立。

顺序来源：`placement_order`（派发时记忆）优先；缺失（Hub 重启）时用排序的 `previous_nodes`。两种来源都确定性，保证跨 tick 稳定。无任何存活节点或无历史时 → 现行 ranked 全量放置（此时没有可保留的东西）。

### D2：派发指纹（内存）收紧跳过条件

`start_dispatch_fingerprints: BTreeMap<(job_id, node_id, generation), u64>`（任务集有序 id 的哈希），派发 start 时写入。跳过条件：存在 Succeeded start **且** 指纹等于当前计算的任务集指纹。失配 → 该 Succeeded start 标 Superseded（复用既有 abandoned-fencing 路径与持久化）→ 下一轮派发新 start。

为什么不持久化指纹：持久化需要 HubOperation 加字段 + 迁移；而"Hub 重启后重派发一次 start"是良性事件（Agent 端一致集 no-op、漂移集替换），一次有界命令换掉一个 schema 变更。指纹内存随 `placement_order` 一起在重启后由首轮派发重建。

### D3：失败观察失效 start（运行时复活入口）

`report_job_observation`（HTTP 层有 auth.node_id）在观测 state 为 `failed` 时，将该节点 (job, generation) 的 Succeeded start 置 `TimedOut` + failure_class `runtime_failed` 并持久化。TimedOut 不满足跳过条件（跳过只认 Succeeded），reconcile 下一 tick 重派；重派的 start 携带 recovery（既有恢复工件选择），内核从 checkpoint 恢复。镜像既有 `invalidate_job_starts_on_boot_change` 的锁序与持久化模式。崩溃循环无内置退避——与显式 restart 的现状一致，观察侧可见（job last_error）。

### D4：Agent 漂移替换（no-op 的精确化）

`JobTask` 增加 `assignment_task_ids: BTreeSet<String>`。同代活内核的 no-op 条件从"generation 相同且存活"收紧为"……且任务集与命令一致"；不一致时落入既有替换路径（cancel + teardown + 带 recovery 重建）。一致集的比较用有序集合相等（顺序无关）。

### D5：诚实边界——split 存活节点的重启不可避免

远程边目标在图构建时固化，节点故障→TCP 断→重连预算耗尽→内核 fail-closed。本变更让该失败后**自动**以稳定映射重启（D3），而非消除重启本身；colocated（默认）存活节点则完全不动。文档明示两种策略的故障窗口差异。

## Risks / Trade-offs

- [复制存活节点导致单点过载] → 仅在无新候选时发生；ranked 候选优先；过载节点会被压力再均衡（opt-in）处理。
- [指纹内存与真实派发脱节（tick 竞态）] → 指纹与派发在同一 reconcile 路径写入，读也在同路径；无并发写者。
- [失败观察反复失效造成重启风暴] → 仅 failed 观测触发；成功观测不触碰；重启后内核健康则不再有 failed 观测。风暴场景 = 持续崩溃，与显式 restart 循环同性质，可观测可人工介入。
- [Agent 替换路径的恢复成本] → 仅在集合真漂移时发生；正常稳态（增量放置成功）零命令。

## Migration Plan

无存储 schema 变更。升级即生效；回滚 = 回退二进制（Succeeded 跳过恢复旧语义，行为回到现状）。

## Open Questions

（无）
