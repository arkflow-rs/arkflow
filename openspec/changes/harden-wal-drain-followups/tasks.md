# Tasks: harden-wal-drain-followups

## 1. 修复与验证

- [ ] 1.1 `wal/mod.rs`：drain 分支改为 `notified` 与 `sleep_until(drain_deadline)` 竞速，计时器先到返回 pending 错误
- [ ] 1.2 `stream_adapter.rs` 竞争测试：GatedAck 增加 entered/release 双信号就绪同步，移除 200ms 兜底超时，close 在确认 entered 后触发
- [ ] 1.3 主 spec `input-durability` drain 需求措辞更新（源失败路径与 drain 超时区分）+ 新增源失败立即阻塞场景

## 2. 验证与收尾

- [ ] 2.1 `cargo test --workspace --all-targets` 连续 2 轮全绿；clippy 无新告警
- [ ] 2.2 `openspec validate harden-wal-drain-followups` 通过；同步主 spec、归档、更新 PLANNING.md、推送
