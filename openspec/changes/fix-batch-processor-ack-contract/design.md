# Design: fix-batch-processor-ack-contract

## Context

batch 处理器的三类缺陷（见 proposal Why 节的 file:line 证据）：缓冲期提前 ack（违反 task.rs:2720-2722 注释明示的 Deferred 契约）、首批 schema 位置式 `concat_batches`、`count` 数批不数行。内核侧机制全部现成：`Processor::process_with_ack`（`processor/mod.rs:82`）、`ProcessResult::{Deferred, SingleWithAck, MultipleWithAck}`、组合 ack 原语 `ConcurrentAck`（`input/mod.rs:415`）、归一合并 `normalize_and_concat`（`component/batch_merge.rs:31`）——本变更是把既有契约套用到该处理器，不发明新机制。

## Goals / Non-Goals

**Goals:**

- 缓冲投递的 ack 延迟到 flush 发射被下游确认后结算；崩溃/取消经 abort 重放。
- 合并走 schema 归一；类型冲突显式失败。
- `count` 按消息行数触发。

**Non-Goals:** 见 proposal Non-goals（不做缓冲持久化、不动 window buffer、不做 count 双模式、不处理 memory/protobuf 的同类 concat）。

## Decisions

### D1 — 缓冲结构改持 `(MessageBatchRef, Arc<dyn Ack>)`，`process_with_ack` 返回 `Deferred`

```rust
struct HeldDelivery { batch: MessageBatchRef, ack: Arc<dyn Ack>, rows: usize }

async fn process_with_ack(&self, msg, ack) -> Result<ProcessResult, Error> {
    self.held.write().await.push(HeldDelivery { batch: msg, ack, rows: msg.len() });
    if self.should_flush_rows().await {
        let (merged, acks) = self.flush_held().await?;      // normalize_and_concat + take acks
        return Ok(ProcessResult::SingleWithAck(
            merged,
            Arc::new(ConcurrentAck(acks)),                   // 下游确认后统一结算
        ));
    }
    Ok(ProcessResult::Deferred)                              // task.rs 对 Deferred 不结算
}
```

- **组合 ack 选 `ConcurrentAck`**：其 ack 语义为依次结算全部子 ack、错误按组合传播（`input/mod.rs:415-455`），正是"一个发射对应 N 个暂存投递"的形状；undo/abort 同步传导。若实现中发现它对 `undo` 的子级补偿顺序有额外要求，允许在 kernel 侧加一个薄包装，但不新增公共 API。
- **`finish()`/`on_tick()` 复用 `flush_held`**：EOS 与空闲超时的发射同样携带 `SingleWithAck`；缓冲为空时返回 `None`（现状）。
- **失败/关闭路径**：处理器 `close` 或链取消时缓冲中未发射的 ack 由持有的 `ConcurrentAck` 等价物逐个 `abort`——处理器需实现 `close`（或依赖内核 FatalFailure 的 acknowledgements 收集路径，实现时取与 window 算子一致的做法）。
- **`process`（无 ack 旧签名）保留**：走无 ack 缓冲（测试/嵌入场景），flush 返回普通 `Single`；两条路径共享 `flush_held` 的合并逻辑。

### D2 — 合并换 `normalize_and_concat`，count 换行数累计

- `flush` 的 `concat_batches(&batch[0].schema(), ..)`（`batch.rs:80-83`）替换为 `normalize_and_concat(&batches)`：字段并集、缺列 null、类型冲突报错（错误信息含冲突列名与两侧类型）。
- 触发条件 `batch.len() >= count`（`batch.rs:58`）改为 `held_rows >= count`（`HeldDelivery.rows` 累计，`MessageBatch::len()` 为行数）；`timeout_ms` 语义不变（最近 flush 起算）。

### D3 — 测试策略

- 内联单测（batch.rs 现有 7 例基础上）：Deferred 期间 ack 未结算、flush 后 ConcurrentAck 结算恰一次、abort 路径、异键序合并、并集合并、类型冲突报错、行数触发。
- 端到端：`executor/tests.rs` 增加 kafka 风格源 + batch 处理器 + 慢 sink 的崩溃重放用例若成本过高，则以 mock ack 断言结算时序替代（与 window 算子既有测试同构）。

## Risks / Trade-offs

- **吞吐影响**：ack 结算点后移到 flush 确认，源 in-flight 窗口（通道 1024）内反向压制缓冲行数——`count` 大于窗口容量时行为从"默默缓冲"变为"受背压节流"。这是契约正确的方向（原本是在拿持久性换吞吐），docs 说明即可。
- **重复语义**：崩溃后重放窗口内消息 → at-least-once（与引擎声明一致）；原本是静默丢失，属修复方向。
- **count 语义变化**：多行批上游的触发时机变早（按行数），单行批上游零变化。

## Migration Plan

单 PR。配置面无新增；`count` 语义收紧在 release notes 中说明（多行批上游的触发行为变化）。

## Open Questions

（无——组合 ack 原语选型在 D1 定案，实现时若 ConcurrentAck 的 undo 顺序需要适配再局部调整。）
