## Why

稳定性专项在并行全量下观察到 `bounded_examples_run_to_completion_on_kernel` 偶发失败（`WAL closed while acknowledgement was pending`，孤立 5/5 通过）：优雅关闭时，被前序 in-flight 源确认挡住的 parked ack 在 `select!` 中被 `close.cancelled` 抢先，立即以错误失败并拖垮整条流——尽管前序交付本可在毫秒级 settle（其源提交成功后 frontier 推进会通知所有 parked 等待者）。语义上 at-least-once 无损（未 settle 的 ack 在恢复时重放），但健康的优雅关闭不应失败流。修复为**有界 drain-on-close**：close 后给 parked ack 一个有界窗口等待前序 settle，超时才回落到既有错误路径（恢复兜底不变）。

## What Changes

- `crates/arkflow-core/src/wal/mod.rs` `acknowledge` 的 parked 分支：`close.cancelled` 触发时不再立即返回错误，而是进入有界 drain 等待（`WAL_CLOSE_DRAIN` 常量，15s）——等待期内前序交付 settle（`ack_notify` 通知）后正常完成本序列的源确认并返回 `Ok`；窗口耗尽仍未能 settle 时返回既有 `WAL closed while acknowledgement was pending` 错误（at-least-once 恢复语义不变）。
- 已 runnable 的确认（`work = Some`）不受影响：close 后仍正常完成其源提交。

## Capabilities

### New Capabilities

<!-- 无新能力：扩展 input-durability。 -->

### Modified Capabilities

- `input-durability`: 优雅关闭时 parked ack 的 drain 语义（有界窗口内完成优于失败）。

## Impact

- `crates/arkflow-core/src/wal/mod.rs`：`acknowledge` parked 分支重写 + 常量 + 集成测试（drain 成功路径；竞争窗口确定性构造）。

## Non-goals

- 不改 close() 的 flusher 语义与 store.close() 时序。
- 不做无界 drain（窗口超时即回落错误，恢复兜底）。
- 不做 per-WAL 可配置窗口（常量起步）。
