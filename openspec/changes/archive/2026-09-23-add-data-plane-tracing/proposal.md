## Why

数据面可观测性（`data-plane-observability`）已有 Prometheus 指标与健康探针，但**没有分布式追踪**——Job 内多条 chain 的生命周期、失败定位与端到端时延在 trace 视图中不可见，跨组件排障只能靠日志拼接。PLANNING.md 7.3-1 的评估（依赖选型、span 切面、OTLP 配置）已具备，本轮精读 `crates/arkflow-core/src/executor/` 后确定插桩点：`run_graph_inner`（Job 编排）与 chain spawn 循环（`task.rs:248-275`）。新增依赖 `opentelemetry`/`opentelemetry_sdk`/`opentelemetry-otlp`(http-json)/`tracing-opentelemetry`，经既有 `tracing` 门面接入，插桩点仅两处且不触碰 worker 池与 wire format。

## What Changes

- 新增依赖（workspace 集中管理）：`opentelemetry`、`opentelemetry_sdk`、`opentelemetry-otlp`（http-json + reqwest client）、`tracing-opentelemetry`；`opentelemetry_sdk` 另为 arkflow-core 的 dev-deps 开 `testing` feature（InMemorySpanExporter）。
- 配置：`ObservabilityConfig` 新增 `tracing` 节（serde default，默认 disabled 零行为变化）：`enabled`/`endpoint`（默认 `http://127.0.0.1:4318/v1/traces`）/`service_name`（默认 `arkflow`）。
- 导出初始化：`cli::init_logging` 的 subscriber 重构为 `Registry + fmt 层 + 可选 OTel 层`（保留文件写入、JSON、pretty、日志级别全部既有行为）；OTLP 批量导出器 + Resource service_name。
- 内核插桩（`executor/task.rs`，仅两处）：
  - `run_graph_inner` 顶部创建并 entered `job.run` 根 span（属性 `chains`）；
  - chain spawn 循环为每条 chain 创建 `chain.run` 子 span（属性 `task`）并随任务移动、在子任务内 enter（跨 tokio 任务显式传递上下文）。
- 静态凭据/审计/指标语义不变。

## Capabilities

### New Capabilities

- `data-plane-tracing`: 数据面 OTel 追踪的配置形状、span 模型（job.run/chain.run）、导出语义（OTLP http-json、批量导出）、默认关闭与失败隔离（导出器故障不影响数据面）。

### Modified Capabilities

<!-- `data-plane-observability` 需求不变：追踪是其旁路扩展，指标/健康探针行为零变化。 -->

## Impact

- `Cargo.toml`：workspace 新增 4 个依赖；`arkflow-core`（含 dev-deps testing feature）与 `arkflow` 引用。
- `crates/arkflow-core/src/config.rs`：`ObservabilityConfig.tracing` 字段 + 默认值。
- `crates/arkflow-core/src/cli/mod.rs`：`init_logging` subscriber 重构 + OTel 层构建。
- `crates/arkflow-core/src/executor/task.rs`：两处插桩（span 创建/enter/移动）。
- 文档：observability 页新增 tracing 节；config-schema/inventory 重新生成。
- 测试：config 默认值；InMemorySpanExporter 集成测试断言 `job.run`/`chain.run` span 存在且父子关系正确、属性正确。

## Non-goals

- 不做 batch/pool 级 span（worker 池跨任务上下文传播需触碰 pool 队列 wire format，v2）。
- 不做跨节点 trace 上下文传播（barrier/envelope 不携带 trace context，wire format 变更另行立项）；每节点每 Job 运行各自为 trace 根。
- 不做 HTTP/gRPC 组件内自动插桩（connector 级 span 后续按需）。
- 不做 OTLP gRPC 协议与采样率配置（v1 固定 parent-based 采样、批量导出 5s 间隔）。
- 不做 tail-based 采样与 span 指标。
