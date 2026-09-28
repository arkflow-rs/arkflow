# Design: Job 资源配额与弱执行隔离

## Context

放置管线现状：`rank_candidates`（`hub/placement.rs`）按 `(有新鲜仪表, 内存可用比例, CPU 余量)` 排序，无占用概念；`JobSpec` 无资源字段。Agent 侧 `spawn_kernel_job` 在共享 tokio runtime 上 spawn 内核任务。资源观测只有 4 个主机级 gauge（`agent.rs` `merge_resource_gauges`：cpu 用量%、内存 used/total/available），缺 CPU 核数容量。

## Goals / Non-Goals

**Goals**：声明的请求参与调度（记账+可行性+有效余量排序）；声明 CPU 的 Job 获得专属有界 runtime；未声明者零行为变化；无新增持久状态。

**Non-Goals**：见 proposal（硬隔离、运行中变更、磁盘/网络、Stream）。

## Decisions

### D1：请求按"每 task"声明，分摊按 assignment 计

`resources: {cpu_millicores: u32, memory_bytes: u64}`（均可选、至少其一）。总量 = 每 task 值 × `plan.tasks.len()`；节点分摊 = 每 task 值 × 该节点 assignment 数。理由：并行度可变（rescale），per-task 是唯一在拓扑变化下保持语义稳定的粒度；colocated 单节点放置自然得到全量分摊。

### D2：占用记账无状态重算，而非增量账本

放置某 Job 时即时重算候选节点的已分配量：遍历 desired-running 的其他 Job，取其当前放置（`placement_order` 记忆，缺失时 previous_nodes）+ 其 plan 的 per-node assignment 数 × 每 task 请求。O(jobs) per candidate，作业数受 `MAX_OPERATIONS`/调度规模约束，可接受。

否决增量账本：dispatch/stop/supersede 各路径记账必然与实际放置漂移（retention 不重派发、Hub 重启丢账本），漂移即过量放置回归——正是要消除的问题。无状态重算永远与"当前认知的放置"一致，且 Hub 重启零恢复成本。

放置顺序歧义（同 tick 多 Job）不影响正确性：每个 Job 用当时最新认知，最坏保守一 tick。

### D3：可行性判定 fail-closed（对声明的 Job），容量缺失 fail-open

- 判定仅对声明了 resources 的作业生效；容量 gauge（`node_cpu_cores`、`node_memory_total_bytes`）缺失或过期的节点**不参与判定**（沿用现状的 fail-open：无仪表不阻塞放置），但排序仍靠后（现状行为）。
- 判定：`(allocated_cpu + this_cpu) ≤ cores × 1000` 且 `(allocated_mem + this_mem) ≤ memory_total × 90%`（10% 系统预留）。
- 候选全部不可行 → `HubError::Invalid("insufficient ... capacity")`，reconcile 本 tick 失败，下 tick 重试——与现有放置校验失败（split 无能力节点）同路径。
- 未声明 resources 的作业跳过判定（零回归）。

### D4：有效余量排序

`headroom_key` 的内存/CPU 输入从观测值改为 `观测 − 已分配`（下限 0）。无仪表节点保持"按 id 序排最后"。已分配信息来自 D2 的重算。未声明作业也受益于该排序（记账仍计入它人声明）。

### D5：专属 runtime = 声明 CPU 的弱执行隔离

`spawn_kernel_job`：`spec.resources.cpu_millicores` 存在时，构建 `tokio::runtime::Builder::new_multi_thread().worker_threads(max(1, ceil(millicores/1000))).build()` 的专属 runtime，内核任务 spawn 于其上；Agent 持有 runtime 句柄于 JobTask，stop 时先 cancel 再 `runtime.shutdown_timeout`。Job 结束时 runtime 销毁。未声明 → 现状共享 runtime。

这是线程级软隔离：约束的是该 Job 可占用的并发 worker 数，不是 CPU 硬顶（cgroup 才是硬顶，Non-goal）。文档如实表述。内存执行面 = 既有 `state.max_bytes` + 专属 runtime 使 Job 的任务堆分配集中（不硬限）。

### D6：`node_cpu_cores` 容量 gauge

`ResourceSnapshot` 增加 `cpu_cores`（sysinfo `cpus().len()`，静态值），`merge_resource_gauges` 输出 `node_cpu_cores`；Hub `ALLOWED_NODE_METRICS` 放行。CPU 容量不新鲜过期问题：核心数静态，按常规仪表新鲜度处理即可（随 report 刷新）。

## Risks / Trade-offs

- [重算成本随作业数增长] → O(jobs×candidates) 每 tick 仅对参与放置的 Job；上限受作业规模现实约束；缓存留作后续优化。
- [无容量 gauge 节点绕过判定] → 与现有"无仪表不阻塞"哲学一致；生产建议 Agent 全量上报（默认即报）。
- [90% 内存预留武断] → 常见保守值；作业可显式声明更小请求补偿；做成常量并文档化。
- [专属 runtime 增加线程开销] → 仅声明 CPU 的 Job；worker 数由声明约束，声明小则开销小。
- [声明值虚报] → 配额是协作式声明，恶意虚报不在威胁模型（操作员控制 spec）。

## Migration Plan

纯增量字段与行为开关：未声明 resources 的作业行为逐位不变。升级后操作员按需为作业声明资源。回滚安全。

## Open Questions

（无）
