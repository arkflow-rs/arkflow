# Tasks: harden-test-stability

## 1. 测试稳定性加固

- [x] 1.1 `executor/remote.rs`：13 处 5s 传播预算统一为 `TEST_PROPAGATION_BUDGET`（30s）
- [x] 1.2 `two_node_job_smoke.rs`：teardown 改为有界等待 + 显式 abort（对齐 fleet_readiness 模式）；条件等待 30s → 60s；agent 停止断言 5s → 15s
- [ ] 1.3 `node_resource_reporting.rs`：无界 await teardown 改为有界等待 + abort
- [x] 1.4 `executor/tests.rs` tick 测试读门 600→1200ms（负载下空闲窗口消失修复，孤立复现 1/5 → 0/6）

## 2. 验证

- [ ] 2.1 `cargo test --workspace --all-targets` 连续 3 轮全绿
- [ ] 2.2 `cargo clippy --workspace --all-targets` 无新告警
- [ ] 2.3 归档 change（无 spec 级变更，无 specs delta）
