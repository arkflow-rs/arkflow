# Tasks: add-cross-node-trace-propagation

## 1. 载体与注入

- [x] 1.1 `CheckpointBarrier.trace_context` 字段 + serde 兼容 + 更新 14 处字面量
- [x] 1.2 `send_downstream` Barrier 分支注入（无效上下文短路）
- [x] 1.3 remote.rs 注入/提取 helper（直接 TraceContextPropagator）

## 2. 恢复与测试

- [x] 2.1 `handle_completed_barrier` 创建 `chain.barrier` span + set_parent
- [x] 2.2 测试：注入往返、wire JSON 双向兼容、端到端 parent 断言、关闭时字节不变
- [x] 2.3 文档（observability en/zh）+ 全量验证 + 归档 + PLANNING + 推送
