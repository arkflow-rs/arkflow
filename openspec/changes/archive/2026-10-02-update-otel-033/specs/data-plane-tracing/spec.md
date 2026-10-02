## ADDED Requirements

### Requirement: 遥测栈升级保持既有追踪契约

OpenTelemetry 依赖家族（opentelemetry / opentelemetry_sdk / opentelemetry-otlp / tracing-opentelemetry）升级 SHALL 保持 ArkFlow 的操作员可见追踪契约：OTLP HTTP-JSON 导出按 `TracingConfig` 配置（endpoint、service_name）工作，W3C trace context 跨节点传播（inject/extract）行为不变，进程关停时的 span 冲刷（shutdown flush）语义不变。语义约定（semconv）span 属性名随上游 0.32 改名（如 `code.filepath`→`code.file.path`、`otel.status_message`→`otel.status_description`）SHALL 视为运维可见变化在发布说明中说明，而非行为回归。

#### Scenario: OTLP 导出配置语义不变

- **WHEN** tracing 启用且配置了 endpoint 与 service_name，span 经 `tracing_opentelemetry` 桥导出
- **THEN** OTLP HTTP-JSON 导出器按配置的 endpoint 发送，Resource 携带 service_name，与升级前一致

#### Scenario: 跨节点 trace 传播不受升级影响

- **WHEN** remote executor 在 job 分发时注入 W3C trace context、对端提取续接
- **THEN** 父子 span 仍串成同一条 trace（`TraceContextPropagator` 的 inject/extract 行为不变）

#### Scenario: 关停冲刷仍工作

- **WHEN** 进程优雅关停调用 `shutdown_otel_tracing`
- **THEN** 缓冲中的 span 被冲刷到已配置的导出器，导出失败被记日志吞掉、不阻塞关停

#### Scenario: semconv 属性改名是运维可见变化

- **WHEN** 升级后下游按旧属性名（如 `code.filepath`）查询 span
- **THEN** 该查询不再命中（属性已改名为 `code.file.path`），发布说明已提前说明改名清单与对应关系
