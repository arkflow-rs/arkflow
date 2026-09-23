# Tasks: add-wal-close-drain

## 1. 实现

- [ ] 1.1 `wal/mod.rs`：`WAL_CLOSE_DRAIN` 常量（15s）+ parked 分支 drain 重写（close 触发后有界等待 ack_notify，settle 后继续循环完成本序列源提交；窗口耗尽回落既有错误）
- [ ] 1.2 集成测试：慢源确认（Notify 释放）+ 并行序列 2 确认 + close → 序列 2 在窗口内完成（确定性竞争构造，无 sleep 依赖）；drain 内序列 1/2 均提交断言

## 2. 文档与全量验证

- [ ] 2.1 input-durability 相关文档页补充优雅关闭 drain 语义（如有对应章节）
- [ ] 2.2 `cargo test --workspace --all-targets` 连续 2 轮全绿；clippy 无新告警；`pnpm docs:check` 通过
- [ ] 2.3 对照场景核对；`openspec validate add-wal-close-drain` 通过；同步主 spec、归档、更新 PLANNING.md、提交
