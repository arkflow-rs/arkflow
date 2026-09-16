## Context

数据面已就绪：`RemoteEdgeContext`（task→node 映射 + 节点数据端口 + manager）驱动 graph 构建产生远程边；Agent 已解析命令载荷中的 `node_data_ports`；节点注册携带 capabilities（含 `network_shuffle`）但**数据端口本身未入注册表**。放置算法 `assignments_for_nodes`（job.rs:706）按连通分量整体落位并附注释声明无跨节点传输。job_start 命令按节点下发各自 assignment 子集（hub.rs:931/1339 → 按 node_id 过滤）。

## Goals / Non-Goals

**Goals:**

- `placement: split` 时 task 粒度多节点分配，数据面真正被消费。
- Hub 侧一次下发全图信息（task→node、节点数据端口），Agent 图构建自足。
- 校验闭环：无数据面能力的节点、跨节点旁路边均拒绝放置。

**Non-Goals:** 动态 rescale、放置策略优化（v1 确定性轮转）、失败通道跨 Job 定界。

## Decisions

**D1：放置配置挂在 `JobSpec.placement`，枚举 `colocated | split`，serde 默认 `colocated`。**
旧 spec JSON 反序列化零影响；`assignments_for_nodes` 按该字段分支。备选"Hub 侧全局配置"被否：放置语义是 Job 的属性（不同 Job 不同策略），且 split 需要 spec 校验联动（旁路边约束）。

**D2：split 分配 = 物理顺序轮转，忽略连通性。**
`assignments_for_nodes` 的 split 分支：遍历 plan.tasks（sources→operators→sinks 的既定顺序），`node_ids[task_index % node_ids.len()]`。连通分量计算仅保留给 colocated 分支。轮转的确定性使任意两节点对同一 placement 输入推导出相同映射（Hub 下发前可自校验）；同 operator 的多个 subtask 相邻编号自然散布到不同节点。备选"按 operator 轮转"被否：会把同 operator 的 subtask 全放一个节点，key-group 分区失去跨节点意义。

**D3：旁路边共位在 `JobSpec::validate` 的 split 分支强制。**
遍历 spec.edges 中 `is_error_sink` 目标与 sources 的 `late_event_route`，若边的两端算子 subtask 在 split 分配下落不同节点 → 校验错误。这在 placement 校验阶段（早于任何命令下发）拒绝，而不是运行期图构建报错。data 边不设限（数据面消化）。

**D4：数据端口入节点注册表，注册时随 capabilities 申报。**
`RegisterRequest` 增加 `data_port: Option<u16>`，存入 `NodeRecord`/持久化 `HubNode`；心跳不重复携带（端口生命周期 = 注册会话，重注册刷新）。`job_start` 的 split 放置校验：期望节点集中任一节点无数据端口或无 `network_shuffle` capability → 拒绝放置。备选"心跳携带"被否：端口随进程绑定，重注册即进程重启，会话级足够。

**D5：job_start 载荷增加 `task_nodes`（全量 task→node）。**
`node_data_ports` 解析 Agent 侧已就绪；补 `task_nodes: {task_id: node_id}` 透传，Agent 的 `RemoteEdgeContext.task_nodes` 从"仅本节点 assignment"升级为全量映射（当前实现查不到远端 task 的 node 会报错——split 下这是必需信息）。载荷为命令 JSON 字段，无独立协议。

**D6：checkpoint 聚合链路复用主干，仅补被动 barrier 模式。**
主干已有 `checkpoint_scope`/`all_nodes_succeeded`/`job_checkpoint_commit` 聚合（hub.rs:3338-3434、agent.rs:861 `aggregate_checkpoint`），split 放置的 assignment 集天然成为期望节点集。**实现期发现一处必需的内核补充**：`kernel_handle::checkpoint_barrier_inner` 原在节点无本地 source（`barrier_senders` 空）时直接报错——split 下 sink-only 节点的 barrier 本应从网络边到达。新增**被动模式**：无本地 source 时不注入，按 barrier 身份匹配等待网络到达的 barrier（等待循环既有 `report.barrier != barrier` 校验保证身份一致）；节点无本地 source 且无 participants 仍报错。e2e（split Job 经真实 TCP 跑通路由 + 聚合 checkpoint）钉住该行为。
已知边界：passive 轮若上游永久不到达，该 kernel 的 checkpoint 锁将保持占用直到 Job 重启（与 colocated 下"链永不报告"的既有暴露一致）；Hub 侧 op 超时保证下一周期重试语义。

## Risks / Trade-offs

- [split 后单节点故障即 Job 级失败，故障面变大] → 既有 generation fencing + 命令重试已覆盖重放置；Job 选择 split 即接受该语义（文档明示）。
- [轮转分配可能把 source 与其重消费者分开造成大流量跨节点] → v1 明示为确定性朴素策略；算子顺序即用户声明的 DAG 顺序，后续策略可插拔。
- [`task_nodes` 全量映射随并行度线性增长命令体] → 数百 task 量级 JSON 可忽略；超大并行度属 Phase 4+ 压缩/分页议题。
- [split 与 colocated 混用集群] → 校验按 Job 独立判定， colocated Job 行为不变（回归门禁）。

## Migration Plan

默认 `colocated`，存量 spec 反序列化零影响；split 需显式配置且全部期望节点具备数据面能力，否则拒绝放置（fail-closed）。回滚 = 改回 colocated 重放置。

## As-Built Notes（实现校准，2026-09-16）

- **`assignments_for_nodes` 改为返回 `Result`**：split 的旁路边违约必须以"拒绝放置"浮出（Hub reconcile 循环里 `panic!` 会杀死整个 reconcile 任务），CR 轮从 panic 改为 `Err`，全部生产调用方（reconcile_job、checkpoint 分发、checkpoint_scope 推导）以 `HubError::Invalid` 干净拒绝。
- **`data_address` 仅存内存注册表**：Hub 重启后持久化节点行不含数据地址，split 放置在该窗口内 fail-closed 拒绝，直至 Agent 重注册（会话 TTL 到期即重注册）。持久化列留给后续 change。
- **被动 barrier 模式**（kernel_handle）：无本地 source 的节点不注入，按 barrier 身份匹配等待网络到达；上游永不到达则该 kernel checkpoint 锁占用至 Job 重启（Hub op 超时保证下一周期重试语义），与 colocated 下"链永不报告"既有暴露一致。

## Open Questions

- `placement` 字段名与取值是否要预留 `custom`（未来策略插件）——实现时定，枚举留余地即可。
