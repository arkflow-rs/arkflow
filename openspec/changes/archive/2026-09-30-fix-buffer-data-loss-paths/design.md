## Context

P1-9 的两个组件都处于"消息已被消费但尚未下发"的中间态，失败/关闭路径缺少排空与补偿纪律（证据见 proposal）。引擎侧的控制分发机制已经齐备：`ProcessorControl::{Finish, Tick}` 经 `dispatch_processor_control` 按链序驱动（`crates/arkflow-core/src/executor/task.rs:2380-2420`），空闲 tick 每 100ms 触发（`task.rs:1290,1320-1337`）；core 已有带补偿语义的 `VecAck`（`crates/arkflow-core/src/input/mod.rs:333-388`）。因此本变更全部落在 `arkflow-plugin` 两个文件内，引擎零改动。

## Goals / Non-Goals

**Goals:**

- batch processor：EOS 排空（`finish()`）、空闲超时刷新（`on_tick()`）、`close()` 非破坏性 + 残留告警。
- memory buffer：合并失败保留队列、capacity 背压、flush/close token 分离、`ArrayAck` → core `VecAck`。

**Non-Goals:**

- 不改 batch `count` 计数语义、不修 schema/文档字段漂移（P2 批次）。
- 不引入 processor 的 `process_with_ack` ack 托管重设计。
- 不动 `buffer/window.rs` 与其他 buffer 实现。

## Decisions

1. **batch 排空走既有控制钩子，而非改 `close()` 签名。** `Processor::close()` 返回 `Result<(), Error>` 无法向下游发数据；`finish()` 返回 `ProcessResult` 正是"EOS 时携带 ack 下发保留数据"的既定钩子（trait 文档 `processor/mod.rs:77-96` 明确写了这个用途）。改 trait 签名波及全部 processor 实现，违反外科手术原则。
2. **超时刷新用 `on_tick()` 而非自建定时任务。** 引擎空闲期已按周期驱动 `on_tick`，在其内部检查 `last_batch_time.elapsed() >= timeout_ms` 即可，避免组件私开任务与关停泄漏。
3. **memory buffer 合并失败保留：先克隆后清除。** `process_messages()` 在写锁内对队列元素做 `concat_batches`（RecordBatch 克隆本来就要做），成功后才 `clear()`；失败路径队列原样返回 `Err`。备选"pop 后失败再 push_back 回填"会打乱顺序且 ack 列表与消息要同时回填，更复杂。
4. **capacity 背压：`write()` 满载时 `select!` 等待容量通知或 close。** 复用现有 `Notify`：读者 `process_messages()` 排空后 `notify_waiters()` 释放写者；`close.cancelled()` 分支保证关停活性（对齐 `stream-backpressure` spec 的 liveness 要求）。备选"write 返回错误"会把背压变成失败，改变语义过猛；备选"无界 + 仅告警"不解决 P1。
5. **flush/close 分离：新增独立 `drain` 信号，`flush()` 不再触碰 `close` token。** `flush()` = `notify_waiters()`（读者醒来排空）；`close()` = cancel token（终止后台计时任务 + 读者排空余量后返回 None）。`flush()` 的"半关闭"副作用就此消失。
6. **直接替换 `ArrayAck` 为 `VecAck`。** `VecAck` 是 core 公共类型且语义即为本变更目标（失败反向 undo、abort、held 标记）；本地结构体没有存在价值，删除而非修补。

## Risks / Trade-offs

- [memory `write()` 阻塞是行为变化：既有测试 `test_memory_buffer_capacity_limit` 顺序写 3 条（capacity=2）会挂起] → 测试改为并发读者形态；文档明示满载阻塞语义。
- [capacity 按"消息条数"（各 batch `len()` 之和）计，与现状一致但写者等待判断在锁内，锁持有时间变长] → 等待发生在**释放写锁之后**的 `select!` 中，锁本身不做长等待，无死锁面。
- [batch `finish()` 在 EOS 下发部分批次，若下游 sink 已关闭则下发失败] → 引擎侧 EOS 时下游仍按序排空（`stream-backpressure` spec "Drains and exits despite backpressure at input EOF"），非本变更新增风险。
- [取消路径下 batch 残留仍会丢（close 无法下发）] → 从"静默丢弃"降级为"带条数的 warn 日志"，属可观测性改善；彻底解决需 ack 托管重设计（Non-goal）。
- [`on_tick()` 刷新使部分批次更早下发，依赖"下一条消息触发"行为的用户会看到下发时机变化] → 与配置文档宣称的 timeout 语义一致，属于修正而非破坏。

## Migration Plan

纯插件内行为变更，无配置面变化。合入后跑 `cargo test -p arkflow-plugin` + workspace 全量 + clippy；回滚即 revert 单个 commit。

## Open Questions

无——capacity 满载"阻塞等待"（而非报错/丢弃）已按 `stream-backpressure` spec 的背压纪律定为唯一合理选项。
