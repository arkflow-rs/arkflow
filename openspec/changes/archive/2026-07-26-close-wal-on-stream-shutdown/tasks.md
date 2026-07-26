## 1. WAL 生命周期实现

- [x] 1.1 在 `Stream::close()` 中纳入可选 WAL 的关闭，确保 stream 任务停止后调用 `Wal::close()`。
- [x] 1.2 按现有组件关闭约定处理并记录 WAL 关闭错误；确认 pending flush 或 flusher join 的失败不会被静默吞掉。

## 2. 回归测试

- [x] 2.1 增加 group-commit 策略的正常关闭测试，验证未到后台 flush 时的消息在重新打开 WAL 后可恢复。
- [x] 2.2 覆盖 periodic 策略或抽取共享测试逻辑，验证关闭会停止 flusher 并完成最终 flush。
- [x] 2.3 运行 `cargo test -p arkflow-core`，确认现有 WAL、Stream 和相关关闭测试全部通过。

## 3. 验证与文档一致性

- [x] 3.1 检查实现与 `input-durability` 规格的 graceful shutdown 场景一致，并确认无 WAL stream 的关闭行为不变。
- [x] 3.2 运行格式化和针对性测试，记录任何无法验证的运行环境限制。
