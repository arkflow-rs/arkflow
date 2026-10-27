# add-wal-flush-failure-metrics

## Why

WAL 后台 flush 失败是「数据静默不持久」的运行时信号，但该计数今天没有任何可观测出口：计数器只活在 `Wal` 内部（`crates/arkflow-core/src/wal/mod.rs:280` 的 `flush_failures: AtomicU64`，`note_flush_failure` 在 flusher wake path 计数），访问器 `flush_failures()` 因「core-api-surface 零引用规则」被 `#[cfg(test)]` 门控（`wal/mod.rs:1151-1154`），`KernelMetricsSnapshot` 与 `/metrics` 渲染层（`crates/arkflow-server/src/metrics.rs` 的 `arkflow_job_*` / `arkflow_stream_*` 词表）均不含该信号。运行态唯一可见的是限流日志（10s 一条），监控系统无法据此告警。这是 PR #1309 coderabbit 复审的遗留立项项（PLANNING.md「已确认决策」节）。

## What Changes

- `crates/arkflow-core/src/wal/mod.rs`：放开 `flush_failures()` / `flush_consecutive_failures()` 的 `#[cfg(test)]` 门控（保持 `pub(crate)`，不扩大公开 API；core-api-surface 零引用规则因出现真实消费者而解除）。
- `crates/arkflow-core/src/job.rs:1401`（`JobComponentAdapter` trait）：新增默认方法 `fn wal(&self) -> Option<Arc<Wal>> { None }`；`StreamJobAdapter` 覆写返回其 `wal` 字段（`stream_adapter.rs:29-36`）。
- `crates/arkflow-core/src/executor/metrics.rs`：`KernelMetrics` 增加 `wal: Mutex<Option<Arc<Wal>>>` 与 `set_wal()`；`KernelMetricsSnapshot` 增加 `#[serde(default)] wal_flush_failures: u64`，snapshot 时从 WAL 读取（scrape 时采样，热路径零开销）。
- `crates/arkflow-core/src/executor/job_runner_adapter.rs`：`run_job_with_metrics_started` 在启动时把 adapter 的 WAL 桥接进 `RuntimeMetrics.kernel`。
- `crates/arkflow-core/src/control.rs`：`StreamMetricsSnapshot` 增加 `#[serde(default)] wal_flush_failures: u64`；`RuntimeMetrics::snapshot` 映射。
- `crates/arkflow-server/src/metrics.rs`：词表新增两条 counter——`arkflow_job_wal_flush_failures_total`（labels `job`，Hub 导出附加 `node`）与 `arkflow_stream_wal_flush_failures_total`（labels `stream_id`）。
- `docs/docs/operate/observability.md` 及其 zh-Hans 对应页：指标词表补两条。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `data-plane-observability`: `/metrics` 数据面词表新增 WAL flush 失败计数（job 与 stream 两级），快照结构经 serde default 保持双向兼容。

## Impact

- `crates/arkflow-core/src/wal/mod.rs`（访问器门控放开）
- `crates/arkflow-core/src/job.rs:1401`（trait 默认方法）、`crates/arkflow-core/src/executor/stream_adapter.rs`（覆写）
- `crates/arkflow-core/src/executor/metrics.rs`、`job_runner_adapter.rs`、`control.rs`、`runtime.rs`（快照映射）
- `crates/arkflow-server/src/metrics.rs`（渲染与测试）；Hub `/metrics` 经既有 `kernel_job_families` + `node` label 路径自动获得，心跳协议无需变更（serde default 兼容）。
- 依赖无变化；不新增采集点，不改 WAL 失败重试/持久化语义。

## Non-goals

- 不暴露 `flush_failures_consecutive` 与 `flush_failure_last_log_ms`（内部限流/升级状态，不进指标）。
- 不改 WAL flush 失败的重试、日志限流或 at-least-once 语义；纯可观测性布线。
- 不加告警规则 / Grafana 面板 / Console 可视化。
- 不做 Agent 心跳协议变更；跨节点部署下 WAL 属 Stream 本地运行时，Agent 侧 Job 无 WAL（其 `wal_flush_failures` 恒 0，不特殊处理）。
