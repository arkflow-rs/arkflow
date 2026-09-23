## Why

coderabbitai 对 add-wal-close-drain 实现的复审指出三处问题：① drain 等待仅 `notified.await`——前序源确认 hang 住时 15s 窗口永不生效（须与截止时间竞速）；② 测试的 GatedAck 可经 200ms 兜底超时完成，导致 drain 路径可能未被真正执行（需就绪信号同步）；③ spec 需区分「前序源失败立即报错」与「drain 超时回落」两种错误路径。

## What Changes

- `wal/mod.rs` drain 分支：`notified` 与 `sleep_until(drain_deadline)` 竞速——通知先到则继续循环，计时器先到则返回 pending 错误。
- `stream_adapter.rs` 竞争测试：GatedAck 增加进入信号（entered Notify），测试等待「序列 1 已 in-flight」后再 close，移除 200ms 兜底超时。
- `input-durability` spec（主 spec 同步）：drain 需求补充「前序源失败立即报错，不经 drain 窗口」场景与措辞。

## Capabilities

### Modified Capabilities

- `input-durability`: drain 语义细化（截止时间强制执行、源失败路径区分、测试确定性）。

## Impact

- `wal/mod.rs` + `stream_adapter.rs` 测试；主 spec input-durability 措辞更新。无产品语义变化（drain 窗口语义按原设计强制生效）。

## Non-goals

- 不做无界 drain；不改 close() 的 flusher 语义。
