# OTel Wire Format Span — 评审级设计

> 状态：评审级设计（未实现）
> 前置：add-data-plane-tracing（job.run + chain.run 生命周期 span 已落地）
> 本文覆盖：batch 级 span、worker pool 上下文传播、跨节点 trace 传播的架构设计和实施计划

## 1. 现有架构分析

### 1.1 数据流管线

```
source.read() → [pool.submit] → worker.process() → [pool.result] → flush_outputs → send_downstream
```

source chain 的 `run_source_chain` 循环内：
- `source.read()` 获取 batch
- batch 经 `ProcessorWorkerPool::submit()` 提交到 pool
- pool worker 完成后结果经 `pool.result` 返回
- `flush_pool_result` → `flush_outputs` → `send_downstream`

interior chain 的 `run_interior_chain_loop` 类似：从上游 channel 收 envelope → dispatch_data → pool → flush。

### 1.2 上下文传播挑战

| 挑战 | 影响 | 解决方向 |
|------|------|---------|
| pool 跨任务 | Span context 不能跨 tokio::spawn 自动传播 | 将 Span 句柄随队列项传递 |
| ordered flush | 结果按序回填，span 结束时间在 flush 点 | 用 `span` 的 `set_attribute` 记录实际处理耗时 |
| barrier 排空 | barrier 期间 pool 结果按序 flush | barrier 前后 span 边界需要区分 |
| hot path 开销 | 无 subscriber 时 span 创建仍有一次 allocation | 使用 `tracing` 的 level filter（span 在 DEBUG 级） |

### 1.3 关键约束

- **不可改 pool 队列 wire format**：pool 队列项 `(batch, ack)` 是 executor 内部协议，扩展需评审
- **不可引入 hot path allocation**：span 创建在 `tracing` disabled 时为零成本
- **ordered delivery 不可破坏**：span 不能改变 batch 的提交/回填顺序

## 2. Batch 级 Span 设计

### 2.1 Span 模型

```
chain.run (parent)
  └── chain.batch (per batch, attributes: rows, seq, task)
```

### 2.2 创建点

source chain 的 `source.read()` Ok 分支：
```rust
let batch_span = tracing::info_span!(
    "chain.batch",
    rows = batch.num_rows(),
    task = %chain.entry_task_id(),
);
let _guard = batch_span.enter();
// ... process + send downstream
```

interior chain 的 `dispatch_data` 函数入口同理。

### 2.3 pool 传播方案

**方案 A：队列项扩展（推荐）**
- pool 队列项从 `(batch, ack)` 扩展为 `(batch, ack, Option<Span>)`
- worker 在 process 时 enter span，完成后 exit
- flush 时 span 已关闭，无泄漏

**方案 B： Span 只在提交点创建，worker 不感知**
- 在 submit 前 enter span，flush 后 exit
- 问题：span 持续时间包含排队等待时间，不反映 worker 处理时间

**推荐方案 A**：准确反映 worker 处理时间，span 随队列项传递。

### 2.4 跨节点传播

跨节点 trace 传播需要将 trace context 编码到 Envelope 中：
- barrier envelope 已有 generation/checkpoint_id，可扩展 `trace_context: Option<Vec<u8>>`
- data envelope 的 wire format 不可变更（帧格式已固定）

**方案**：在 barrier frame 中携带 trace context，下游节点在处理 barrier 时恢复 trace 上下文。data batch 的 trace 传播通过 barrier 间接实现。

### 2.5 性能评估

| 开销来源 | 估计 | 缓解 |
|---------|------|------|
| span 创建（有 subscriber） | ~50ns | 可接受 |
| span 创建（无 subscriber） | 0 | tracing 零成本抽象 |
| 属性设置 | ~20ns | 可接受 |
| OTLP 导出 | 异步批量 | 不阻塞热路径 |

### 2.6 实施计划

| 阶段 | 内容 | 依赖 |
|------|------|------|
| 1 | batch 级 span（source chain） | 无 |
| 2 | batch 级 span（interior chain） | 阶段 1 |
| 3 | pool 上下文传播（方案 A） | 阶段 1-2 |
| 4 | 跨节点传播（barrier envelope） | 阶段 3 + wire format 评审 |
