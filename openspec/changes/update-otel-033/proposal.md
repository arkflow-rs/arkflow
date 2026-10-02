## Why

依赖赛道最后一站可动项：OpenTelemetry 家族 0.28 → 0.33（`tracing-opentelemetry` 0.29 → 0.34）。当前锁定 `opentelemetry = "0.28"`（`Cargo.toml:59-62`，四 crate 全家 0.28/0.29）。上游锁里**只有 ArkFlow 自己的 crate 依赖 otel**（`cargo tree -i opentelemetry@0.28.0` 证实，无 ballista/datafusion 连坐），升级完全自主。

收益不只是"追新"：
- 桥 0.33 修复**进入 span 时死锁**——我们在 remote executor 热路径高频进 span（`crates/arkflow-core/src/executor/remote.rs` 的传播/instrument 路径）
- otel 0.31 恢复批量导出**真并行**（`OTEL_BSP_MAX_CONCURRENT_EXPORTS` 此前不生效）
- 0.33 是官方奔向 1.0 的收口站（计划 0.33.1 宣布 API 稳定）——现在落地，未来 1.0 是廉价一跳

## What Changes

- 四 crate 锁步：`opentelemetry`/`opentelemetry_sdk`/`opentelemetry-otlp` 0.28→0.33，`tracing-opentelemetry` 0.29→0.34（配伍矩阵：桥 0.34 ↔ otel 0.33）。
- **确定的代码迁移点**：`crates/arkflow-core/src/cli/mod.rs:439-443` 的 `.with_export_config(ExportConfig { endpoint, protocol, ..default })` —— `ExportConfig`/`with_export_config()` 在 otlp 0.32 被移出公共 API，改为 `WithExportConfig` trait 链式方法（`.with_endpoint(...)` 等）。
- 编译驱动小修：`provider.tracer("arkflow")`（0.30 beta 可能改 Scope 签名）、测试侧 `InMemorySpanExporter`（0.32/0.33 重命名 + 错误类型换 `OTelSdkError`），涉及 `crates/arkflow-core/src/executor/tests.rs`。
- **不变**（changelog 逐一核对）：`SpanExporter::builder().with_http()`（0.33 移除的是 transport-first 构建器，我们已在 blessed 路径）、`SdkTracerProvider::builder().with_batch_exporter().with_resource()`、`Resource::builder()`、`TraceContextPropagator` inject/extract（remote.rs 跨节点传播零破坏）、`layer().with_tracer()`/`OpenTelemetrySpanExt`。
- **运维注意（发布说明项）**：0.32 起 semconv 属性改名（`code.filepath`→`code.file.path`、`otel.status_message`→`otel.status_description` 等），代码无需改，但下游按旧属性名建的看板需更新。

## Non-goals

- 不采纳新特性：不加 metrics 导出、不加 probabilistic sampling、不改采样策略。
- 不改 `TracingConfig` YAML 表面（`endpoint`/`service_name` 语义不变，无 docs/README/ schema 影响）。
- 不升 opentelemetry-semantic-conventions 独立依赖（我们未直接使用）。
- 不处理 reqwest 副本外观问题（若 otlp 0.33 的 reqwest 区间引入第二副本：记录、不阻塞、卫生守卫不覆盖）。
- 不等待 otel 1.0（先落 0.33 收口站）。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `data-plane-tracing`：新增一条需求——遥测栈升级保持既有追踪契约（OTLP 导出配置语义、W3C 跨节点传播、关停冲刷行为不变；semconv 属性改名作为运维可见变化入场景）。

## Impact

- **代码**：`crates/arkflow-core/src/cli/mod.rs`（初始化 ~15 行重写）、`crates/arkflow-core/src/executor/tests.rs`（测试导出器小改）、可能的零散编译修复。
- **依赖**：workspace `Cargo.toml` 四行版本串；`Cargo.lock` 家族整体前移。
- **运维**：semconv 属性名变化影响下游看板（发布说明）；运行时受益于桥死锁修复与批量导出并行化。
