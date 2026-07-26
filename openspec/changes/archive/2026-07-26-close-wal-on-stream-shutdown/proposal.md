## Why

启用 WAL 的 stream 在正常关闭时没有调用 `Wal::close()`。对于 `group-commit` 和 `periodic` 策略，最近写入的数据可能仍停留在内存 pending 队列中，导致优雅停机也丢失本应刷入 WAL 的消息。需要将 WAL 纳入 Stream 的统一关闭流程，使正常关闭具备可验证的持久化语义。

## What Changes

- 将配置启用的 WAL 关闭纳入 `Stream::close()`，在 stream 处理任务停止后停止后台 flusher 并刷出 pending 数据。
- 按现有组件关闭风格记录 WAL 关闭或最终 flush 的错误，不静默丢弃关闭失败。
- 增加回归测试，验证 group-commit/periodic 策略下正常关闭后 pending WAL 数据可在重新打开时恢复。
- 不改变崩溃、断电场景下各同步策略已经声明的 durability trade-off。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `input-durability`: 正常关闭时必须完成 WAL pending 数据的刷盘并停止后台 flusher。

## Impact

- 影响 `crates/arkflow-core/src/stream/mod.rs` 的 stream 生命周期管理。
- 可能调整 `crates/arkflow-core/src/wal/mod.rs` 的关闭错误处理。
- 增加或扩展 `arkflow-core` 的 WAL/Stream 测试。
- 不新增依赖，不改变配置格式、WAL 文件格式或公开配置 API。
