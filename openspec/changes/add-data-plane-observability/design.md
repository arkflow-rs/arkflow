# Design: add-data-plane-observability

## Context

统一内核已在 `KernelMetrics`（`crates/arkflow-core/src/executor/metrics.rs`）产出 per-chain 计数与 checkpoint/watermark/迟到事件聚合，但出口断在三层：

1. **本地模式**：YAML `jobs` 由 `Engine::run_with_cancellation` 在 spawn 的任务里经 `run_job_with_checkpoints_started` 执行（`crates/arkflow-core/src/engine/mod.rs:99`），`KernelJobHandle`（含 `metrics()`，`executor/kernel_handle.rs:496`）留在任务内部，Engine 不持有；本地 `/metrics`（`crates/arkflow-server/src/lib.rs:3123`）只遍历 legacy Stream 快照。
2. **Agent 模式**：Agent 持有 `KernelJobHandle` 并在心跳里上报**节点级聚合**的扁平 map（`crates/arkflow-server/src/agent.rs:344`，键为 `kernel_batches_in` 等，跨 Job 求和），Hub 存下后只在 JSON 诊断中出现（`lib.rs:899`、`lib.rs:2404`）；Hub `/metrics`（`lib.rs:2333`）只导出控制面序列。
3. **进程入口**：`serve()` 在 `server.enabled=false` 时立即返回（`lib.rs:258`），纯数据面部署没有任何可观测端点。

约束：`arkflow-core` 不持有 HTTP 传输（`engine/mod.rs:14-15` 注释明确传输归 `arkflow-server`）；`prometheus = "0.13"` 已在 workspace；现有 `arkflow_stream_*` 序列、`/readiness`、`/liveness` 必须保持兼容。

## Goals / Non-Goals

**Goals:**

- 本地 `/metrics` 与独立 observability 监听导出统一内核 Job 指标（per-chain + checkpoint + watermark + late events），带 HELP/TYPE 的合法 Prometheus 0.0.4 文本。
- `server.enabled=false` 时进程仍提供 `/metrics`、`/ready`、`/live`（独立最小监听）。
- Hub `/metrics` 导出 Agent 上报的 per-Job 数据面指标（`node` + Job/chain 标签）。
- 全部现有端点路径与序列保持不变。

**Non-Goals:**

- OpenTelemetry trace、新采集点、per-plugin 指标、Console 可视化、remote write（见 proposal Non-goals）。

## Decisions

### D1. 指标收集注册表放在 arkflow-core 的 RuntimeManager

新增 `JobMetricsRegistry`（`Arc<Mutex<BTreeMap<String /* job_id */, Arc<KernelMetrics>>>>`），由 `RuntimeManager` 持有，`ControlPlane` 经现有 `runtime_manager()` 可达。

- `run_job_with_checkpoints_started` 增加一个可选 registry 参数：spawn 成功后 `register(job_id, handle.metrics())`，job 任务结束（watcher 返回后）注销。现有调用方传 `None` 即可，签名变化是加参而非破坏行为。
- Engine 为每个 YAML Job 传入共享 registry；编译为 JobSpec 的 Streams 不注册——它们已由 legacy `arkflow_stream_*` 序列覆盖，避免同一数据双导出。

备选：让 runner 把 `KernelJobHandle` 送回 Engine（extra oneshot）——调用方要在 job 结束前持有 handle，与现有「spawn 后 await watcher」结构冲突；registry 更简单且 Agent 路径未来可复用。

### D2. 导出用 prometheus crate 的 proto + TextEncoder，按 scrape 快照构建

在 `arkflow-server/src/metrics.rs`（新模块）实现 `render_data_plane_metrics(...)`：每次请求时从 `KernelMetrics::snapshot()` 与 Stream 快照构建 `prometheus::proto::MetricFamily`（正确声明 counter/gauge），`TextEncoder` 输出 0.0.4 文本。

- 不使用全局 `Registry` + 常驻 Collector：Job 启停频繁时注册/反注册有竞态与泄漏面；快照构建无状态、与 `KernelMetrics::snapshot()` 的读取模型一致。
- 替换现有手拼文本路径，顺带为 legacy `arkflow_stream_*` 补上 HELP/TYPE（名字、标签、含义不变）。

序列命名（词表，标签仅 job/chain/stream/node）：

| 序列 | 类型 | 标签 |
| --- | --- | --- |
| `arkflow_job_chain_batches_in_total` / `_batches_out_total` / `_rows_total` / `_errors_total` | counter | job, chain |
| `arkflow_job_chain_in_flight` / `_mean_latency_us` | gauge | job, chain |
| `arkflow_job_checkpoint_duration_ms` / `_watermark_lag_ms` | gauge | job |
| `arkflow_job_checkpoint_failures_total` / `_late_events_total` | counter | job |

### D3. 独立 observability 监听，`/ready`/`/live` 作为统一路径

新增 `serve_observability(control_plane, config, cancellation)`（arkflow-server）：

- `server.observability.enabled`（默认 **true**，绑 `127.0.0.1:8081`，路径可配）：main.rs 始终 spawn 它；当 API server 启用时**不**再额外监听，由现有 router 提供 `/metrics`，并新增 `/ready`、`/live` 路由（`/readiness`、`/liveness` 原样保留，语义不变）。
- `/ready` 复用 `control_plane.health()`：本地模式下 Engine 在 streams+jobs 全部启动后才 `set_ready(true)`（`engine/mod.rs:142`），语义即「数据面就绪」；`/live` 恒为存活（进程内 handler）。
- 最小监听只挂 `/metrics`、`/ready`、`/live` 三个 handler，无资源 API、无认证（默认 loopback，不暴露配置内容）。

备选：默认关闭 observability——满足「issue #768 开箱即用」诉求弱；绑 loopback 已规避暴露风险。

### D4. Agent report 增加 per-Job 快照字段（serde default，向后兼容）

`NodeReport` 新增可选字段 `jobs: BTreeMap<String, KernelMetricsSnapshot>`（serde default）：Agent 按 Job 汇报完整快照（不再预聚合），现有扁平 `kernel_*` 聚合键**保留**（Hub 诊断与旧 Hub 兼容）。数据随 report 传输（metrics 本就承载于 report；heartbeat 仅存活状态）。Hub 存储按 (node, job) 保留最近一次快照，`hub_metrics` 追加数据面 family（同一套 D2 渲染函数，family 加 `node` 标签）。

- 旧 Agent → 新 Hub：字段缺失 = 无数据面序列，不报错；新 Agent → 旧 Hub：未知字段被忽略。无强制升级顺序。
- payload 上限：每 Job 一次快照，规模 = 节点上的 Job 数 × chain 数（配置有界），心跳周期不变。

### D5. 端点授权边界

- 独立监听与本地 router 的 `/metrics`、`/ready`、`/live`：无认证（现状如此），依赖默认 loopback 绑定；文档明确生产部署应显式配置绑定地址/防火墙。
- Hub `/metrics` 数据面序列：跟随现有 `hub_metrics` 的 operator Bearer 认证（`lib.rs:2337`），不新增认证模型。

## Risks / Trade-offs

- **标签基数失控** → job/chain/stream/node 均为配置有界标识；D2 词表封闭，测试断言「错误消息不产生新序列」；文档标注基数量级 = O(jobs × chains)。
- **Report payload 增大** → 仅新增 per-Job 快照，受配置约束；若未来 Job 数大，可在 Hub 侧聚合（非本次范围）。
- **`run_job_with_checkpoints_started` 加参扩散调用点** → 编译期全部暴露；`None` 保持旧行为，调用方仅 engine/mod.rs、agent 适配层与测试。
- **`/ready` 与 `/readiness` 并存易混淆** → 文档声明 `/ready` 为推荐路径、`/readiness` 仅为兼容；二者在本地模式语义一致（同一 health 状态）。
- **默认开启监听的端口冲突** → 绑定失败只告警不阻断引擎（数据面可用性优先），错误信息给出配置项名。

## Migration Plan

纯增量发布：新配置字段均有默认值；旧序列与端点不动。回滚 = 关闭 `server.observability.enabled`（行为回到现状）。Hub/Agent 混布无升级顺序约束（D4 双向兼容）。

## Open Questions

- 无（OTel trace 已明确为后续独立 change）。
