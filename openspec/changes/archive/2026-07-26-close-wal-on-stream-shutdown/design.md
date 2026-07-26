## Context

`Stream` 持有可选的 `Arc<Wal>`，输入任务负责 append，`WalAck` 在下游确认后推进 cursor。当前 `Stream::close()` 只关闭 input、buffer、pipeline 和 output，没有停止 WAL flusher。`group-commit` 与 `periodic` 策略的 append 可能仍在 pending 队列中，释放最后一个 `Arc<Wal>` 不会执行异步 flush。

关闭流程已经在 `run()` 中等待所有 stream 任务结束，因此可以在组件关闭阶段安全地停止 WAL。实现应保持现有组件“记录错误并继续关闭”的行为。

## Goals / Non-Goals

**Goals:**

- 将 WAL 关闭纳入 `Stream::close()`。
- 正常关闭时停止后台 flusher，并完成 pending 数据的最终刷盘。
- 记录 WAL 关闭失败，避免错误被静默丢弃。
- 用回归测试验证 group-commit/periodic 的 pending 数据在关闭后可恢复。

**Non-Goals:**

- 不改变 WAL 文件格式、配置格式或同步策略语义。
- 不保证进程崩溃、断电时 group-commit/periodic 没有丢失窗口。
- 不新增自动重试、WAL 压缩或 cursor 清理机制。

## Decisions

1. **在 `Stream::close()` 中关闭 WAL。**
   `Stream` 已经统一管理输入、处理和输出资源，WAL 属于该 stream 的 durability 资源，应在同一生命周期出口关闭。备选是在 `run()` 中单独调用，但会分散生命周期逻辑，并可能遗漏其他调用 `Stream::close()` 的路径。

2. **在业务任务结束后执行 WAL 关闭。**
   `run()` 已先等待 `TaskTracker`，因此不会再有输入任务向 WAL append；随后关闭组件并最终关闭 WAL，避免在处理阶段提前终止 WAL。具体实现应确保 output/ack 使用的 WAL 引用已不再需要。

3. **保持关闭错误的“记录并继续”策略。**
   现有组件 close 错误会记录后继续关闭其他组件。WAL close 也采用同样策略；必要时调整 `Wal::close()` 内部被忽略的 flush 错误，使其通过返回值暴露给 `Stream`。

4. **增加生命周期回归测试而非只测试 `Wal::close()`。**
   现有 WAL 单元测试已经覆盖直接调用 close 的行为，新增测试应覆盖生产关闭链路，证明 `Stream::close()` 或等价关闭流程实际触发 pending flush。

## Risks / Trade-offs

- [关闭时 flush 失败] → 记录错误并保留现有关闭流程，测试覆盖错误可观察性；不伪造成功。
- [仍有外部 `WalAck` 引用] → 仅在任务停止、缓冲数据完成处理后关闭 WAL，并验证 Arc 生命周期不会阻止 flusher 退出。
- [关闭延迟增加] → 只在启用 WAL 的 stream 关闭时等待一次最终 flush；无 WAL 的 stream 不增加路径。

## Migration Plan

无需配置或数据迁移。部署新版本后，启用 WAL 的 stream 在正常关闭时自动执行最终 flush。若回滚，旧版本仍可打开现有 WAL 文件；回滚只会恢复原有的正常关闭 pending 数据风险。

## Open Questions

无。
