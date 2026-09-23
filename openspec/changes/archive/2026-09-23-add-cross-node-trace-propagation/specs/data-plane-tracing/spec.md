# data-plane-tracing 变更（Delta）

## ADDED Requirements

### Requirement: 跨节点 barrier trace 传播

追踪启用时，barrier 离开链任务时 SHALL 注入当前上下文的 W3C traceparent（`CheckpointBarrier.trace_context`，wire JSON 双向兼容：旧节点可读、无值时字段不出现）；下游节点处理对齐完成 barrier 时 SHALL 创建 `chain.barrier` span（属性 `task`/`checkpoint_id`/`generation`），`trace_context` 存在时其父 SHALL 为远端上下文；追踪关闭时 SHALL 不注入不改字节、无 span 开销。多跳路径上每个开启追踪的节点以本节点 barrier span 上下文延续链路，未开启节点透传。

#### Scenario: 下游 barrier span 关联远端 trace

- **WHEN** 节点 A 在活动 span 内发出 barrier 且节点 B 处理其对齐完成
- **THEN** 节点 B 导出的 chain.barrier span 的 parent 为节点 A 该 barrier 离开时的 trace 上下文

#### Scenario: 追踪关闭时字节与行为不变

- **WHEN** 注入方与接收方均未启用追踪
- **THEN** barrier 的 wire JSON 不含 trace_context 字段，处理路径无 span 创建开销

#### Scenario: 未开启追踪的中间节点透传

- **WHEN** barrier 携带 trace_context 经过一个未启用追踪的节点
- **THEN** 该节点原样转发 trace_context，链路在下一跳继续
