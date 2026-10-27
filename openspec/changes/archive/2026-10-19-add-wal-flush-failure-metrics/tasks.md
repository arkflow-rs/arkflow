# add-wal-flush-failure-metrics — Tasks

## 1. WAL 访问器放开

- [x] 1.1 `crates/arkflow-core/src/wal/mod.rs`：移除 `flush_failures()` 的 `#[cfg(test)]` 门控，保持 `pub(crate)`，更新 doc comment 说明消费者为 metrics snapshot（`KernelMetrics::snapshot`）；`flush_consecutive_failures()` 保留 `#[cfg(test)]`（不进指标，放开会触发 dead-code 告警）

## 2. 快照与布线（arkflow-core）

- [x] 2.1 `crates/arkflow-core/src/executor/metrics.rs`：`KernelMetrics` 增加 `wal_flush_failures: std::sync::Mutex<Option<Arc<AtomicU64>>>`（`Default` 派生保持）与 `pub(crate) fn set_wal(&self, wal: &Arc<Wal>)`（只取 `flush_failures_handle()`：共享 WAL 会延长 redb 独占锁，共享计数器则不会，见 design D1）；`KernelMetricsSnapshot` 增加 `#[serde(default)] pub wal_flush_failures: u64`，`snapshot()` 从计数器句柄读取（无 WAL 时为 0）
- [x] 2.2 `crates/arkflow-core/src/job.rs:1401`（`JobComponentAdapter` trait）：新增默认方法 `fn wal(&self) -> Option<Arc<crate::wal::Wal>> { None }`
- [x] 2.3 `crates/arkflow-core/src/executor/stream_adapter.rs`：`impl JobComponentAdapter for StreamJobAdapter` 覆写 `wal()` 返回 `self.wal.clone()`
- [x] 2.4 `crates/arkflow-core/src/executor/job_runner_adapter.rs`：`run_job_with_metrics_started` 在 `run_graph_with_metrics_startup` 之前把 `adapter.wal()` 注入 `metrics.kernel.set_wal(...)`（`metrics` 与 `wal` 同时为 `Some` 时）
- [x] 2.5 `crates/arkflow-core/src/control.rs`：`StreamMetricsSnapshot` 增加 `#[serde(default)] pub wal_flush_failures: u64`
- [x] 2.6 `crates/arkflow-core/src/runtime.rs`：`RuntimeMetrics::snapshot` 把 `kernel.wal_flush_failures` 映射进 `StreamMetricsSnapshot`

## 3. 渲染层（arkflow-server）

- [x] 3.1 `crates/arkflow-server/src/metrics.rs`：`kernel_job_families` 新增 `arkflow_job_wal_flush_failures_total`（counter，labels `job` + 可选 `node`，值取 `snapshot.wal_flush_failures`）
- [x] 3.2 `crates/arkflow-server/src/metrics.rs`：`stream_families` 新增 `arkflow_stream_wal_flush_failures_total`（counter，label `stream_id`）
- [x] 3.3 更新既有断言：`counters_declared_with_total_names_and_gauges_without` 覆盖两条新 counter 的 TYPE；§2.4 的封闭 label 词汇断言自动覆盖，无需改动

## 4. 测试

- [x] 4.1 `metrics.rs` 单测：snapshot 绑定 WAL 后计数随 WAL flush 失败增长（可用 `Wal` 测试设施或注入 stub 计数路径）；未绑定 WAL 的快照 `wal_flush_failures == 0`
- [x] 4.2 `metrics.rs` 单测：Job/Stream 两级新 family 的名称、TYPE、label 与取值断言（job 级含 `node` label 变体）
- [x] 4.3 serde 兼容单测：`KernelMetricsSnapshot` / `StreamMetricsSnapshot` 在 JSON 缺字段时反序列化为 0（复用既有 serde 测试位置）
- [x] 4.4 `crates/arkflow-core` 端到端断言：带 WAL 的 Stream 运行路径上，`RuntimeMetrics::snapshot().wal_flush_failures` 可读到 WAL 计数（无失败时为 0，验证线已接通；与既有 `wal/mod.rs` 失败注入测试组合）；runner 接线由 `run_job_with_metrics_started_binds_the_adapters_wal` 断言 `wal_bound()`，计数器在 WAL drop 后保持

## 5. 文档（en + zh-Hans）

- [x] 5.1 `docs/docs/operate/observability.md`：指标词表新增两行（`arkflow_job_wal_flush_failures_total` / `arkflow_stream_wal_flush_failures_total`），并在「何时告警」条目补 WAL flush 失败持续增长的含义
- [x] 5.2 zh-Hans 对应页（`docs/i18n/zh-Hans/...` 下同路径文件）同步

## 6. 收口验证

- [x] 6.1 `cargo test -p arkflow-core` 与 `cargo test -p arkflow-server` 全绿；`cargo clippy --workspace --all-targets` 无新增告警
- [x] 6.2 `cargo test --workspace --all-targets`（CI 形态）通过；`pnpm docs:check` 通过
- [x] 6.3 实跑验证：带 durability 的 stream 配置启动进程，`curl /metrics` 能看到 `arkflow_stream_wal_flush_failures_total{stream_id=...} 0`（线已接通且不破坏既有 exposition）
