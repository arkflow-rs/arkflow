# Capability: Data-plane Tracing

## Purpose

Define the OTel trace export for the unified-kernel data plane: the `observability.tracing` configuration (disabled by default), the `job.run`/`chain.run`/`chain.batch` lifecycle span model with parent linkage, OTLP http-json batch export, failure isolation, and the guarantees that existing logging/metrics behavior is unchanged. (Purpose derived from the `add-data-plane-tracing` change; refine as the capability evolves.)

## Requirements

### Requirement: OTel 追踪配置与默认关闭

观测配置 SHALL 新增 `tracing` 节：`enabled`（默认 `false`）、`endpoint`（默认 `http://127.0.0.1:4318/v1/traces`）、`service_name`（默认 `arkflow`）。`enabled=false`（含缺省）时 SHALL 不构建任何 OTel 对象、不发任何网络请求，进程行为与日志输出 SHALL 与现状一致；exporter 构建失败时 SHALL 记录警告并继续运行（数据面不受影响）。

#### Scenario: 默认配置零行为变化

- **WHEN** 配置不含 `tracing` 节
- **THEN** 反序列化成功且 `enabled=false`，进程不发起 OTLP 请求，日志输出不变

#### Scenario: exporter 构建失败隔离

- **WHEN** OTLP endpoint 非法导致 exporter 构建失败
- **THEN** 进程记录警告后正常运行，流/Job 不受影响

### Requirement: Job 与 chain 生命周期 span

追踪启用时，统一内核 SHALL 为每次图执行创建 `job.run` 根 span（属性 `chains`=链数），并为每条 chain 创建 `chain.run` 子 span（属性 `task`=链入口任务 id），父子关系 SHALL 正确；chain span SHALL 在子任务内通过显式传递的 span 进入（tokio 子任务不继承上下文）。span 生命周期 SHALL 覆盖对应执行期：`job.run` 覆盖图运行全程，`chain.run` 覆盖该链从启动到资源关闭。

#### Scenario: 最小图的 span 结构

- **WHEN** 运行一个含单 source chain 的流且追踪启用
- **THEN** 导出的 span 含 `job.run` 与 `chain.run`，`chain.run` 的 parent 为 `job.run`，`task` 属性为该链入口任务 id，`job.run` 的 `chains` 属性为 1

#### Scenario: span 覆盖执行期

- **WHEN** 一条 chain 从启动运行到资源关闭
- **THEN** 对应 `chain.run` span 的持续时间覆盖该链执行期，链内发出的日志事件挂为该 span 的事件

### Requirement: OTLP 导出与既有行为保持

追踪启用时 span SHALL 经 OTLP http-json 批量导出到 `endpoint`，Resource 的 `service.name` 为 `service_name`；既有日志行为（文件写入、JSON/pretty 格式、日志级别过滤）SHALL 逐字节保留；指标与健康探针端点行为 SHALL 不变。

#### Scenario: 导出目标与资源属性

- **WHEN** 配置 `endpoint` 与 `service_name` 后运行并产生 span
- **THEN** span 以 OTLP http-json 批量导出到该 endpoint，Resource `service.name` 为配置值

#### Scenario: 既有日志行为保持

- **WHEN** 追踪启停两种状态下分别以文件+JSON 与控制台+plain 配置运行
- **THEN** 日志的落点、格式与级别过滤行为一致

### Requirement: batch 级处理 span

追踪启用时，统一内核 SHALL 为 source chain 中每个从 source 读取的 batch 创建 `chain.batch` 子 span（parent 为 chain.run），属性含 `rows`（batch 行数）和 `task`（链入口任务 id）。span SHALL 覆盖从 batch 进入 pipeline 到发送 downstream 的全程。关闭追踪时不产生任何额外开销。

#### Scenario: batch span 包含 rows 和 task 属性

- **WHEN** source chain 读取一个 10 行的 batch
- **THEN** 导出的 chain.batch span 包含 `rows=10` 和 `task=<入口任务id>` 属性

#### Scenario: 关闭追踪时零开销

- **WHEN** OTel tracing 未启用
- **THEN** batch 处理路径无 span 创建开销（tracing subscriber 丢弃 span）

### Requirement: wire format span 评审级设计文档

SHALL 产出评审级设计文档，覆盖 batch 级 span 切面、worker pool 上下文传播、跨节点 trace 传播的架构设计和实现计划。

#### Scenario: 设计文档覆盖全部架构维度

- **WHEN** 查阅 wire format span 设计文档
- **THEN** 包含 batch span 切面、pool 传播方案、跨节点传播方案、性能评估、实施计划

### Requirement: 跨节点 barrier trace 传播

追踪启用时，barrier 离开链任务时 SHALL 注入当前上下文的 W3C traceparent（`CheckpointBarrier.trace_context`，wire JSON 双向兼容：旧节点可读、无值时字段不出现）；下游节点处理对齐完成 barrier 时 SHALL 创建 `chain.barrier` span（属性 `task`/`checkpoint_id`/`generation`），`trace_context` 存在时其父 SHALL 为远端上下文；追踪关闭时 SHALL 不注入不改字节、无 span 开销。多跳路径上每个开启追踪的节点以本节点 barrier span 上下文延续链路，未开启节点透传。trace 上下文 SHALL NOT 参与 barrier 身份比较（身份仅 checkpoint_id + generation）。

#### Scenario: 下游 barrier span 关联远端 trace

- **WHEN** 节点 A 在活动 span 内发出 barrier 且节点 B 处理其对齐完成
- **THEN** 节点 B 导出的 chain.barrier span 的 parent 为节点 A 该 barrier 离开时的 trace 上下文

#### Scenario: 追踪关闭时字节与行为不变

- **WHEN** 注入方与接收方均未启用追踪
- **THEN** barrier 的 wire JSON 不含 trace_context 字段，处理路径无 span 创建开销

#### Scenario: 未开启追踪的中间节点透传

- **WHEN** barrier 携带 trace_context 经过一个未启用追踪的节点
- **THEN** 该节点原样转发 trace_context，链路在下一跳继续
