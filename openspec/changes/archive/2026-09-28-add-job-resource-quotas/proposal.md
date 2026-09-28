## Why

调度目前只按节点观测仪表排序（`hub/placement.rs` 的 `rank_candidates`/`headroom_key`，键为内存可用比例与 CPU 余量），**没有资源请求模型、没有占用量记账、没有放置可行性判定**——`JobSpec`（`job.rs:389-423`）无任何资源字段，节点资源不足时照常放置，形成过量堆叠。执行侧 Job 全部共享 Agent 进程的 tokio runtime（`agent.rs` `spawn_kernel_job` 直接 `tokio::spawn`），一个重 Job 可占满全部 worker 线程，与其他 Job 之间无任何执行隔离。

## What Changes

- `JobSpec.resources`（可选）：`cpu_millicores` 与 `memory_bytes`，语义为**每 task 请求**；作业总请求 = 字段 × 计划任务数，节点分摊 = × 该节点 assignment 数。缺省（未声明）= 不参与记账与判定，行为与现状逐位一致。
- Agent 资源采样新增 `node_cpu_cores` 容量 gauge（Hub 侧 metric 白名单同步放行）；内存容量沿用既有 `node_memory_total_bytes`。
- Hub 放置升级：
  - **占用记账（无状态重算）**：每次放置时按“desired-running 作业 × 其当前放置（placement_order/previous_nodes）× 每任务请求”即时重算各候选节点的已分配 CPU 毫核与内存字节——无新增持久状态，Hub 重启后自然自愈。
  - **可行性判定**：候选节点需满足（已分配 + 本作业在该节点的分摊）≤ 容量（CPU 毫核 ≤ cores×1000，内存 ≤ total 的 90% 预留系统余量）；无容量 gauge 的节点不参与判定（维持现状 fail-open）。全量无可行节点 → 放置以显式资源不足错误失败（下 tick 重试），不再盲目堆叠。
  - **排序升级**：headroom 键从观测值改为“观测值 − 已分配”的有效余量。
- Agent 执行隔离（弱形式，诚实交付）：声明了 `cpu_millicores` 的 Job 在**专属有界 tokio runtime** 上执行，worker 线程数 = `max(1, ceil(cpu_millicores/1000))`——一个 Job 无法占满共享 runtime 的全部 worker；未声明的 Job 维持现状（共享 runtime）。内存执行面维持既有 `state.max_bytes` 状态预算；堆内存硬限不声称。
- Non-goal（显式）：cgroup/容器/独立进程级硬隔离——多 Job 仍共享 Agent 进程地址空间；本变更交付的是调度层配额 + runtime 线程级弱隔离，文档明示边界。

## Capabilities

### New Capabilities

- `job-resource-quotas`: Job 资源请求声明、节点占用记账、放置可行性判定、有效余量排序、专属 runtime 执行隔离。

### Modified Capabilities

- `resource-aware-placement`: 排序与放置需求从"观测仪表排序"扩展为"扣除已分配量的有效余量排序 + 可行性判定"。

## Impact

- `crates/arkflow-core/src/job.rs`：`JobResourceSpec` 字段与解析。
- `crates/arkflow-server/src/agent.rs`：`node_cpu_cores` gauge；声明 CPU 的 Job 专属 runtime 执行。
- `crates/arkflow-server/src/hub/nodes.rs`：metric 白名单。
- `crates/arkflow-server/src/hub/placement.rs`：占用重算、可行性过滤、有效余量排序、资源不足错误。
- 测试：字段解析、记账正确性（多作业分摊）、可行性拒绝与放行、排序有效性、runtime worker 数、未声明零回归。
- 文档（en + zh-Hans）：资源配置、调度行为、隔离边界。

## Non-goals

- 不做 cgroup/容器/子进程级硬隔离（地址空间、堆内存、CPU 硬顶）——多 Job 共享 Agent 进程。
- 不做运行中作业的资源限制变更（改 resources 走既有的 stop→upgrade→start）。
- 不做磁盘/网络带宽配额。
- 不改 YAML Stream（单机形态）的资源模型。
