## 1. TrackingAck 终态守卫

- [x] 1.1 `crates/arkflow-core/src/executor/commit.rs`：`TrackingAck` 增加 `aborted: AtomicBool`；`abort` 置位、`undo` 入口终态 no-op（含不调 `inner.undo()`）；正常 undo/abort 语义与错误文案不变
- [x] 1.2 单测：undo-after-abort 后 `blocking()==0`；连续 abort 幂等；正常 undo 回退不变
- [x] 1.3 `state_journal.rs:2678` 既有测试补 tracker 层计数断言（抓回归）

## 2. WAL 驻留租约

- [x] 2.1 `crates/arkflow-core/src/wal/mod.rs`：`WAL_ACK_PARK_TIMEOUT`（60s，`AtomicU64` 可注入覆盖，与 `ack_drain_window_ms` 同法）；驻留等待改三路 `select!`（notify / 超时 / close 既有 drain 窗口）；超时返回可重试错误，文案含卡住的序号
- [x] 2.2 单测：注入短租约——gap 持有者不结算时驻留者超时收错；租约内结算则正常放行；close 窗口语义不变（既有测试原样）
- [x] 2.3 与 `fix-object-store-wal-loss-window` 的密封等待上界（`flush_interval×4+5s`）复核常量关系（吞吐档 45s < 60s 不冲突），在两处常量注释互相引用

## 3. 文档与门禁

- [x] 3.1 checkpoint/WAL 排障页（en/zh）补驻留超时的失败模式说明（显式周期性错误替代静默停滞）
- [x] 3.2 门禁：`cargo test -p arkflow-core` 全绿；`cargo clippy --workspace --all-targets` 无新告警；`cargo fmt --all`
- [x] 3.3 契约复核：两个 delta spec 每个 Scenario 有测试证据；tasks 勾选
