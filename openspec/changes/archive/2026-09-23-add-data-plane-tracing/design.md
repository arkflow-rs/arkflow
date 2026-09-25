## Context

统一内核的执行骨架：`run_graph_inner`（`executor/task.rs:166`）完成资源连接后为每条 chain spawn 任务（`task.rs:248-275`），`run_chain`（`:314`）捕获 panic 并关闭组件，批处理在 `run_source_chain`/`run_interior_chain_loop` 中进行，重负载路径经 `ProcessorWorkerPool`（跨任务提交，ordered 结果回 flush）。日志走 `tracing`，全局 subscriber 在 `cli::init_logging`（`cli/mod.rs:321`）以 `fmt::Subscriber` 直接 set_global_default。

约束：`set_global_default` 只能调用一次（测试需用 `with_default` 作用域订阅者）；tokio 子任务不继承 thread-local span 上下文——跨任务必须显式传递 span；worker 池 wire format 不可触碰。

## Goals / Non-Goals

**Goals:**
- Job 与 chain 的生命周期 trace：`job.run` 根 span → 每链 `chain.run` 子 span，含属性与正确父子关系。
- 默认关闭；开启后 OTLP http-json 批量导出，导出器故障不影响数据面。
- 既有日志行为（文件/JSON/pretty/级别）逐字节保留。

**Non-Goals:** batch/pool 级 span、跨节点传播、connector 插桩、gRPC OTLP、采样配置、tail 采样（见 proposal）。

## Decisions

1. **span 模型 v1 = 两个生命周期 span**：`job.run`（`run_graph_inner` 顶部创建并 entered，属性 `chains`）与 `chain.run`（spawn 循环内创建——此时 job span 已 entered，父子关系自然建立；随任务 move、在子任务内 enter，解决 tokio 子任务不继承上下文的问题）。备选 batch 级 span——**拒绝**：批处理经 worker 池跨任务，正确的 submit→flush span 需要把上下文随队列传递（wire format 变更）；而在 flush 点建 span 的时长只覆盖 flush，是误导性数据。v1 的两个生命周期 span 已回答「哪条链活了多久、何时失败」。
2. **失败可见性**：span 本身不携带 status（tracing-opentelemetry 限制），既有 `tracing::warn!`/error 事件在 span 上下文内发出，自然挂为 span event；v1 不额外加 error 事件点（run_chain 退出路径已有 warn）。
3. **导出初始化位置**：`init_logging` 内（唯一 set_global_default 处）。重构为 `Registry::default().with(fmt_layer).with(otel_layer)`：fmt 层用 `tracing_subscriber::filter::LevelFilter` 承接原 max_level 语义（file/JSON/pretty 三个变体分别构造），otel 层无级别过滤（span 采集由 enabled 开关与未来采样控制）。OTLP http-json + reqwest client，批量导出器默认 5s 间隔——进程常驻下延迟可接受；provider 存入 `opentelemetry::global`（once 语义，与单次 init_logging 匹配）。
4. **配置形状**：`ObservabilityConfig.tracing: TracingConfig`，`enabled`（默认 false）、`endpoint`（默认 `http://127.0.0.1:4318/v1/traces`）、`service_name`（默认 `arkflow`）。serde default → 存量配置零变化；`enabled=false` 时不构建任何 OTel 对象、不发网络请求。
5. **失败隔离**：exporter 构建失败 → 记 warn、OTel 层为 None（数据面继续）；运行期导出失败由 SDK batch exporter 内部处理（重试/丢弃），不反压链路。
6. **依赖选型**：`opentelemetry`/`opentelemetry_sdk`/`opentelemetry-otlp` 0.28 线 + `tracing-opentelemetry` 0.29（官方兼容矩阵）；otlp features `http-json`+`reqwest-client`（复用 reqwest，避免 hyper 直连栈）。InMemorySpanExporter 仅测试用（sdk `testing` feature，dev-deps）。
7. **测试策略**：a) config 默认值/反序列化；b) InMemorySpanExporter + `Registry.with_default` 作用域订阅者跑一个最小真实 graph（source chain 读 memory batch），断言导出 span 含 `job.run` 与 `chain.run`、`chain.run` 的 parent 为 `job.run`、`task` 属性正确——**不触碰 global provider**（绕开 set_global_default 单次限制与并行测试互扰）；c) 既有 init_logging 行为由现有 CLI 测试与全量回归兜底。

## Risks / Trade-offs

- [两个生命周期 span 的 trace 粒度粗] → v1 诚实交付骨架；batch/worker 级传播是 v2（wire format 变更需 openspec 评审）。
- [批量导出导致进程退出丢最后 5s span] → 常驻进程可接受；优雅退出 flush 属后续（provider 全局 once 语义使精确 flush 复杂化）。
- [otel 依赖树较重（约 +30 crates）] → 一次性成本，全部 optional 路径（enabled=false 不触碰）；选 http-json 而非 tonic 显著瘦身。
- [init_logging 重构回归] → 行为矩阵（file×format×level）由手工矩阵核对 + 全量回归；OTel 层默认 None 时 Registry+fmt 层输出与旧 fmt::Subscriber 等价。
- [global provider once 限制] → init_logging 仅在进程启动调用一次（既有契约）；测试绕开 global。

## Migration Plan

默认关闭零行为变化；开启即纯增量导出。回滚 = 删除 tracing 配置节（或 enabled=false）。

## Open Questions

无。
