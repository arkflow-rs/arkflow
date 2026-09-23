## Why

data-plane-tracing 已实现 job.run/chain.run 双生命周期 span，但缺少 batch 级处理 span——运维排障时需要看到每个 batch 在哪些 operator 处理了多久、哪些 operator 是瓶颈。本变更在 chain 处理热路径上添加 batch 级 span，复用已有 OTel 基础设施。

## What Changes

- 在 chain 处理循环中为每个 batch 创建 `chain.batch` 子 span（parent 为 chain.run），属性含 `rows`、`task`（入口任务 id）；span 在 batch 从 source 读取到发送 downstream 的全程覆盖。
- 添加 `tracing::info!` 事件记录 operator 处理错误（含 operator id），自动挂为 chain.run span 的事件。

## Capabilities

### New Capabilities

<!-- 无新能力：扩展 data-plane-tracing。 -->

### Modified Capabilities

- `data-plane-tracing`: 新增 batch 级 span 和 operator 错误事件需求。

## Impact

- `crates/arkflow-core/src/executor/task.rs`：source chain 和 interior chain 的 batch 处理路径添加 span。
- 文档：data-plane-tracing spec 扩展。

## Non-goals

- 不做跨节点传播（barrier/envelope wire format 变更）。
- 不做 operator 级独立 span（chain 内部 operator 由同一个 task 处理）。
- 不做 pool worker 级 span（pool 内部的 span 传播需要改 pool 队列 wire format）。
