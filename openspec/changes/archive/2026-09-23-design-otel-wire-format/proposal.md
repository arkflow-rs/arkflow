## Why

data-plane-tracing 已实现 job.run/chain.run 生命周期 span，但缺少 batch 级处理 span 和跨节点 trace 上下文传播。OTel wire format span 评审级设计需要精读统一内核（task.rs/graph.rs/envelope.rs/barrier.rs 共 3100+ 行）后才能产出评审级设计，确保不引入性能回归或正确性风险。

## What Changes

- 产出评审级设计文档（`openspec/specs/data-plane-tracing/design.md`），覆盖：
  - batch 级 span 切面设计（source/interior chain 的 batch 进入/退出点）
  - worker pool 上下文传播方案（pool 队列项扩展 vs 替代方案）
  - 跨节点传播方案（barrier envelope 扩展 vs 独立 signal frame）
  - 性能影响评估（span 创建开销、热路径影响、采样策略）
  - 实现计划（分阶段：batch span → pool 传播 → 跨节点传播）
- 不做代码实现（纯设计文档 + 决策记录）。

## Capabilities

### New Capabilities

<!-- 无新能力：纯设计文档。 -->

### Modified Capabilities

<!-- 无需求级变更。 -->

## Impact

- 产出设计文档：`openspec/specs/data-plane-tracing/design.md`。
- 为后续实现提供评审基础和实施路径。
