# Tasks: add-data-plane-tracing

## 1. 依赖与配置

- [x] 1.1 workspace `Cargo.toml` 新增 `opentelemetry = "0.28"`、`opentelemetry_sdk = { version = "0.28" }`、`opentelemetry-otlp = { version = "0.28", features = ["http-json", "reqwest-client"] }`、`tracing-opentelemetry = "0.29"`；`arkflow-core`/`arkflow` 引用，core dev-deps 开 sdk `testing` feature
- [x] 1.2 `ObservabilityConfig` 新增 `tracing: TracingConfig`（enabled 默认 false / endpoint 默认 OTLP http-json 地址 / service_name 默认 arkflow）+ 默认值单测
- [x] 1.3 `ARKFLOW_REGENERATE_DOCS=1` 重新生成 config-schema/inventory

## 2. 内核插桩与导出初始化

- [x] 2.1 `executor/task.rs`：`run_graph_inner` 顶部创建并 entered `job.run`（属性 chains）；chain spawn 循环创建 `chain.run`（属性 task）并 move 进子任务 enter
- [x] 2.2 OTel 层构建：exporter 失败隔离（warn 后 None）；`cli::init_logging` 重构为 Registry + fmt 层（file/JSON/pretty/级别全保留）+ 可选 otel 层
- [x] 2.3 InMemorySpanExporter 集成测试：最小真实 graph 运行后断言 `job.run`/`chain.run` 存在、父子关系、`task`/`chains` 属性；不触碰 global provider

## 3. 文档与全量验证

- [x] 3.1 observability 文档页新增 tracing 节（配置表、span 模型、v1 边界：无 batch/跨节点传播）
- [x] 3.2 `cargo test --workspace --all-targets` 全绿；`cargo clippy --workspace --all-targets` 无新告警；`pnpm docs:check` 通过
- [x] 3.3 对照 specs/data-plane-tracing 场景逐条核对；`openspec validate add-data-plane-tracing` 通过
- [x] 3.4 同步主 spec `openspec/specs/data-plane-tracing/spec.md`、归档 change、更新 PLANNING.md、提交
