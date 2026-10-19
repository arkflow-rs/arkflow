# add-wal-flush-failure-metrics — Design

## Context

WAL 后台 flusher 的失败计数已存在且语义成熟：`Wal::flush_failures: AtomicU64` 在 wake path 由 `note_flush_failure` 累加（`crates/arkflow-core/src/wal/mod.rs:1113-1144`），成功重置 consecutive 计数、超阈值升级日志级别、限流一条/10s。该计数是「staged entries 未持久化」的唯一结构性信号。但访问器 `flush_failures()` / `flush_consecutive_failures()` 因 core-api-surface 零引用规则被 `#[cfg(test)]` 门控（`wal/mod.rs:1151-1161`），快照与导出链路（`KernelMetricsSnapshot` → `crates/arkflow-server/src/metrics.rs` → `/metrics`）不含它，监控系统只能靠日志告警。

数据面导出已有两级词汇（`arkflow_job_*`、`arkflow_stream_*`，label 词汇封闭于 `node`/`job`/`chain`/`stream_id`，`metrics.rs:1-7`）。Stream 经 `RuntimeMetrics`（含 `kernel: Arc<KernelMetrics>`，`runtime.rs:214`）导出 legacy 系列；本地 Job 经 `JobMetricsRegistry` 注册 `KernelMetrics` 导出 `arkflow_job_*`；Hub 侧由 Agent 心跳携带 per-Job `KernelMetricsSnapshot`（serde）经 `kernel_job_families` + `node` label 导出。

关键现状约束：

- WAL 只属于 Stream（`StreamJobAdapter` 的 `wal: Option<Arc<Wal>>`，`stream_adapter.rs:29-36`）；本地 Job 与 Agent 侧 Job 均以 `StreamJobAdapter::new(None)` 构建（`engine/mod.rs:132`），无 WAL。
- `KernelMetrics` 目前是纯计数器结构（`executor/metrics.rs:62-72`），不持有外部资源引用；`snapshot()` 在 scrape / 心跳时调用。

## Goals / Non-Goals

**Goals:**

- `/metrics`（本地与 Hub 两级）能暴露 WAL 后台 flush 失败累计数：`arkflow_stream_wal_flush_failures_total`（stream 级）与 `arkflow_job_wal_flush_failures_total`（job 级）。
- 布线对热路径零开销：计数采样发生在 scrape 时，不在每批数据处理路径。
- 协议兼容：`KernelMetricsSnapshot` / `StreamMetricsSnapshot` 新增字段 serde default，旧 Agent ↔ 新 Hub 及反向部署均无损。

**Non-Goals:**

- 不暴露 consecutive 计数与最后日志时间戳；不改 WAL 重试/持久化/日志语义；不加告警规则与 Console 可视化；不做心跳协议版本变更。

## Decisions

### D1 — 桥接载体：`KernelMetrics` 持有 `Arc<Wal>`，snapshot 时读

`KernelMetrics` 增加 `wal_flush_failures: std::sync::Mutex<Option<Arc<AtomicU64>>>`，`snapshot()` 读句柄输出 `wal_flush_failures: u64`。Prometheus counter 的采样模型与 scrape 周期天然匹配：flusher 在后台任务中计数，快照侧只在 scrape/心跳时原子读取一次，数据路径（chain 循环、ack 路径）零新增指令。

**共享计数器而非共享 WAL**：`Wal.flush_failures` 由 `AtomicU64` 改为 `Arc<AtomicU64>`，`set_wal` 只保留 `flush_failures_handle()`。两个约束同时满足——① 若持有 `Arc<Wal>`，WAL 生命周期被延长，redb store 独占锁在 Stream 停止后仍被持有，重启报 `Database already open`（实施时被 `runtime::durability_tests::durability_enabled_start_reopens_the_same_wal_path` 实测抓获）；② 若持 `Weak<Wal>`，WAL 一 drop 快照就回到 0，而已停止的 Stream 仍在 `RuntimeManager::snapshots()` 中导出（`api/diagnostics.rs:452`），活 series 出现计数回退。共享计数器只延长计数器分配的生命周期，不碰锁，停止后总量保持，重启时新 WAL 产生新计数器（重置语义与重新注册一致，文档已说明）。

备选：每批推送（chain 循环读 WAL 计数 store 进 AtomicU64）——需要改 chain 循环或 ChainHooks，热路径每批一次读，且计数更新粒度受批次节奏约束（长空闲期失败要等下一批才可见）；否决。

`Mutex<Option<Arc<AtomicU64>>>` 仅启动时写一次、snapshot 时读，std Mutex 足够（无 await 跨越）；`Option` 表达「该 job/stream 无 WAL」。

### D2 — 注入点：`JobComponentAdapter` 默认方法 + `run_job_with_metrics_started` 接线

在 `JobComponentAdapter` trait（`crates/arkflow-core/src/job.rs:1401`）加默认方法 `fn wal(&self) -> Option<Arc<Wal>> { None }`（零破坏：既有实现者无需改动），`StreamJobAdapter` 覆写返回 `self.wal.clone()`。`run_job_with_metrics_started`（`job_runner_adapter.rs:300`）在 run 前执行 `if let (Some(metrics), Some(wal)) = (&metrics, adapter.wal()) { metrics.kernel.set_wal(wal); }`。

备选 A：在调用点（`runtime.rs:676`）直接设——adapter 在该处是具体类型，可行但把「metrics 与 WAL 的关联」这一运行器职责泄漏给调用方，且未来新增 WAL 形态的调用点需各自记得接线；否决。

备选 B：`run_graph_with_metrics_startup` 里对 `chain.source` downcast 找 `WalInput`——`Input` trait 无 `as_any`（`input/mod.rs:275`），需要给 trait 加方法、扩散面更大，且 graph 构建期 source 可能是非 WAL 包装的其它形态；否决。

### D3 — 词表：两条 counter，label 词汇保持封闭

- `arkflow_stream_wal_flush_failures_total{stream_id}` — 唯一真实产生值的路径（WAL 属 Stream）。
- `arkflow_job_wal_flush_failures_total{job[, node]}` — 本地 Job 与 Agent Job 恒 0（无 WAL），按 Prometheus 惯例保留系列（counter 自 0 起），不特殊抑制；系列存在本身即「该 job 无 WAL 维度」。

命名沿用既有模式（counter 带 `_total` 后缀、gauge 不带，`metrics.rs:311-330` 有测试钉死该约定）。渲染实现复用 `family()` 助手，label 集不引入任何新键（`exposition_is_parseable_and_values_match_the_snapshot` 测试的封闭词汇断言自动覆盖）。

### D4 — 访问器形态：生产路径走计数器句柄，按值访问器保持测试专用

生产消费者（`KernelMetrics::snapshot`）读 `flush_failures_handle()` 返回的共享 `Arc<AtomicU64>`，不需要 `Wal` 上的按值访问器。`flush_failures()` 与 `flush_consecutive_failures()` 因此都保持 `pub(crate) + #[cfg(test)]`：前者供测试断言总量，后者本就不进指标。二者在非测试构建中无消费者，放开会触发 dead-code 告警，正是 core-api-surface 零引用规则要防的情况。公开 API 面零新增。

### D5 — serde 兼容

`KernelMetricsSnapshot.wal_flush_failures` 与 `StreamMetricsSnapshot.wal_flush_failures` 均加 `#[serde(default)]`，与 `kernel_chains`（`control.rs:89-90`）同模式。Hub/Agent 混版本部署无需协调顺序。

## Risks / Trade-offs

- [KernelMetrics 持有 WAL 引用延长 WAL 生命周期，超出 Stream 运行期] → 只共享 `Arc<AtomicU64>` 计数器句柄：快照不需要 WAL 存活，redb 独占锁随最后一个 WAL 强引用释放（`durability_enabled_start_reopens_the_same_wal_path` 钉死重启路径），而停止的 Stream 仍报告最后总量（series 仍在导出）。
- [公开类型失去 `Debug`] → `Wal` 不实现 `Debug` 且不应为此扩展其 API 面；`KernelMetrics` 手写 `Debug` 实现，输出 chain 键集合、WAL 是否绑定与各原子字段，保住公开 API 兼容性。
- [Job 级系列恒 0 造成导出噪音] → 接受：Prometheus 惯例下 counter 从 0 开始输出；恒 0 系列同时也是「该 job 无 WAL」的显式事实，帮助运维区分「无 WAL」与「无导出」。
- [Mutex 在 snapshot 热路径] → 启动后只读；若未来担心 scrape 频率下的锁争用，可退化为 `OnceLock` 或启动期 set 后 store 进 AtomicU64 的惰性快照，本 change 不做（无证据需要）。
- [访问器放开后被误当公开契约] → 保持 `pub(crate)` + 注释更新，说明消费者为 metrics snapshot；归档时核对 core-api-surface spec 无新增公开项。

## Migration Plan

无数据迁移、无配置变化。发布顺序：单 PR 合入即可；旧 Hub 渲染新 Agent 快照（无该字段 → serde default 0）与新 Hub 渲染旧 Agent 快照均安全，无需协调部署。

## Open Questions

（无——各决策均基于现状代码直接成立）
