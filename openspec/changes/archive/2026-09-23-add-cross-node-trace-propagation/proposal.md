## Why

design-otel-wire-format 评审级设计（openspec/specs/data-plane-tracing/design.md 阶段 4）的落地：跨节点 trace 传播。当前每个节点为本地 Job 各自作为 trace 根，跨节点的 barrier 处理在追踪视图中断链，无法回答「一个 checkpoint 从 source 到最远 agent 的全链路耗时」。

## 设计评审结论（对 design.md 2.4 的具体化修订）

1. **载体具体化**：barrier 在 wire 上以 JSON `WireSignal::Barrier` 序列化（非固定帧），因此 `trace_context: Option<String>`（W3C traceparent）直接挂在 `CheckpointBarrier` 上，`#[serde(default)]` + `skip_serializing_if` 双向兼容旧节点；Arrow 数据帧格式保持不动，与设计约束一致。
2. **注入点修订**：不在 pump（泵任务无活动 span），而在 `send_downstream` 的 Barrier 分支——链任务上下文（chain.run/chain.barrier）在发送时仍活跃，注入的是正确的本地 span 上下文；当前上下文无效（追踪关闭）时不注入、不改字节。
3. **恢复点具体化**：interior 链的对齐完成处理 `handle_completed_barrier` 创建 `chain.barrier` span（属性 task/checkpoint_id/generation），`trace_context` 存在时 `set_parent` 指向远端上下文，否则为本地 chain.run 子 span。多跳：本节点转发时 send_downstream 以本节点 barrier span 上下文重写 trace_context，链路逐跳延续；无追踪节点透传保留原值。
4. **传播器**：直接实例化 `TraceContextPropagator`（不依赖 global propagator 配置）。

## What Changes

- `CheckpointBarrier` 增加 `trace_context: Option<String>`（serde 兼容），更新全部 14 处字面量。
- `send_downstream` Barrier 分支注入当前上下文的 traceparent（无效上下文短路）。
- `handle_completed_barrier` 创建 `chain.barrier` span 并按远端上下文 set_parent。
- 测试：注入/提取往返、wire JSON 双向兼容、经真实 chain 循环的端到端 parent 断言。

## Capabilities

### Modified Capabilities

- `data-plane-tracing`: 新增跨节点 barrier trace 传播需求。

## Impact

- `crates/arkflow-core/src/checkpoint.rs`、`executor/task.rs`、`executor/remote.rs`、`executor/tests.rs`
- 文档：observability.md（en/zh）边界描述更新；data-plane-tracing spec 扩展。

## Non-goals

- 不在数据帧（Arrow IPC）上传播 trace 上下文。
- 不改 pool 队列 wire format。
- 不做采样策略、不做 per-barrier 链路以外的 connector 级 span。
