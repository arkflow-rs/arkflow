## Context

ArkFlow 的遥测面很窄：一个初始化函数（`crates/arkflow-core/src/cli/mod.rs` 的 `build_otel_layer`，~45 行）、跨节点传播（`executor/remote.rs` 用 `TraceContextPropagator` inject/extract）、几处 span 属性设置、测试侧内存导出器。上游在 0.28→0.33 之间经历 5 个破坏性 minor（0.30 是 trace API/SDK 首个 beta 大重构），但对照 changelog 逐项核验后，真正命中我们代码的只有一处主力点 + 编译驱动的零星小修。锁中仅我们自己的 crate 依赖 otel，无生态连坐。

配伍矩阵（锁步对）：opentelemetry 0.33 + opentelemetry_sdk 0.33 + opentelemetry-otlp 0.33 + tracing-opentelemetry 0.34。

## Goals / Non-Goals

**Goals：**

- 四 crate 一次锁步到 0.33/0.34，落在奔向 1.0 的收口站。
- 保持操作员可见追踪契约（OTLP 导出语义、W3C 传播、关停冲刷）逐场景不变。
- 把 semconv 属性改名作为运维可见变化显式记录（PR 描述 + 发布说明），不留给下游自己发现。

**Non-Goals：**

- 不采纳新特性（metrics、采样策略）；不改 `TracingConfig` YAML 表面；不等 1.0。
- 不为 reqwest 可能的第二副本引入手工 pin（记录即可，卫生守卫不覆盖 reqwest）。

## Decisions

1. **单 PR 直升 0.33，不做中间版本逗留。** 0.29–0.32 没有我们需要的中间停留价值；中间版本同样是破坏性升级，分步只会重复编译驱动工作量。
2. **导出器配置重写为 trait 链式方法。** `cli/mod.rs:437-444` 从 `.with_export_config(ExportConfig { endpoint, protocol, ..default })` 改为 `.with_http().with_endpoint(config.endpoint.clone())` + 协议设置（otlp 0.33 上 `Protocol::HttpJson` 通过构建器的对应方法指定；确切方法名以编译器/文档为准，属机械替换）。`Protocol::default()` 在 0.32 起不再读环境变量——我们显式设置协议，不受影响，且行为更确定。
   - *备选*：继续用环境变量 `OTEL_EXPORTER_OTLP_PROTOCOL`——否决：与现有 `TracingConfig` 显式配置哲学相悖。
3. **测试导出器跟随 0.32/0.33 重命名。** `InMemorySpanExporter` 相关调用点（`executor/tests.rs`）按编译错误改；错误类型断言如有按 `OTelSdkError` 调整。testing feature 在 0.33 变为 runtime 无关，测试只会更简单。
4. **传播层零改动作为验证目标而非假设。** `TraceContextPropagator`/`TextMapPropagator` 在 changelog 中无破坏记录，但以现有传播测试（跨节点 barrier trace 传播场景）通过为准，不删测试。
5. **semconv 改名清单进 PR 描述。** 上游 0.32 的改名（`code.filepath`→`code.file.path`、`code.lineno`→`code.line.number`、`code.namespace`→`code.module.name`、`otel.status_message`→`otel.status_description`）逐条列给运维。

## Risks / Trade-offs

- [0.30 beta 重构可能有 changelog 未逐条覆盖的签名变化（`tracer()`/`Value` 构造）] → 全部是编译驱动小修；现有 1098 个 core 测试 + 遥测相关测试是行为回归网。
- [otlp 0.33 的 `reqwest-client` 特性区间可能与 workspace reqwest 不重叠，产生第二 reqwest 副本] → 实施时检查锁 diff；副本属外观问题，记录不阻塞。
- [下游看板按旧 semconv 属性名查询失效] → PR 描述 + 发布说明给改名对照表；本仓库无 docs 页面受影响（`TracingConfig` 表面不变）。
- [桥 0.34 发布仅数周（2026-09-20）] → 桥的变更内容仅为对接 otel 0.33（上游 changelog 明示无 `layer()`/`with_tracer`/`set_attribute` 变化），风险面小。

## Migration Plan

单 PR 于分支 `deps/otel-033`：四行版本串 + `cargo update` + `build_otel_layer` 重写 + 编译驱动小修 + 测试全绿。回滚 = revert；无磁盘格式、无配置表面变化。发布说明附带 semconv 改名对照。
