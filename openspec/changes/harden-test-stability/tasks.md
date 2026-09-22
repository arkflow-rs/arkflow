# Tasks: harden-test-stability

## 1. 测试稳定性加固

- [ ] 1.1 `executor/remote.rs`：13 处 5s 传播预算统一为 `TEST_PROPAGATION_BUDGET`（30s）
- [ ] 1.2 `two_node_job_smoke.rs`：teardown 改为有界等待 + 显式 abort（对齐 fleet_readiness 模式）；条件等待 30s → 60s；agent 停止断言 5s → 15s
- [ ] 1.3 `node_resource_reporting.rs`：无界 await teardown 改为有界等待 + abort

## 2. 验证

- [ ] 2.1 `cargo test --workspace --all-targets` 连续 3 轮全绿
- [ ] 2.2 `cargo clippy --workspace --all-targets` 无新告警
- [ ] 2.3 归档 change（无 spec 级变更，无 specs delta）
