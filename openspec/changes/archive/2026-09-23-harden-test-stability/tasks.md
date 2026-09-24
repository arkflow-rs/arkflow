# Tasks: harden-test-stability

## 1. 测试稳定性加固

- [x] 1.1 `executor/remote.rs`：13 处 5s 传播预算统一为 `TEST_PROPAGATION_BUDGET`（30s）
- [x] 1.2 `two_node_job_smoke.rs`：teardown 改为有界等待 + 显式 abort（对齐 fleet_readiness 模式）；条件等待 30s → 60s；agent 停止断言 5s → 15s
- [x] 1.3 `node_resource_reporting.rs`：无界 await teardown 改为有界等待 + abort
- [x] 1.4 `executor/tests.rs` tick 测试读门 600→1200ms（负载下空闲窗口消失修复，孤立复现 1/5 → 0/6）

## 2. 验证

- [x] 2.1 `cargo test --workspace --all-targets` 连续 3 轮全绿（v3 前曾复现 checkpoint 单次触发失速，已加周期性重触发修复）
- [x] 2.2 `cargo clippy --workspace --all-targets` 无新告警
- [x] 2.3 归档 change（无 spec 级变更，无 specs delta）

### 后续修复（2026-09-24）

`tick_output_does_not_overtake_in_flight_pooled_data` 在 CI 上仍偶发失败（本文档此前的读门 600→1200ms 修复未覆盖该变体的两种剩余时序竞争：启动窗口内 idle tick 累积 2 个 leading tick；负载下两个 sleep 一起拉伸导致空闲窗口消失、tick 完全缺失）。已改为确定性门控：TickMarkerProcessor 以 `PublishGate`（Mutex 标志 + Notify，防丢失唤醒）在首个 batch 处理完成时放行输入的第二个 batch——空闲窗口由构造保证而非墙钟余量；on_tick 在首个 process 开始前不产输出。10 连跑 + 全量 sweep 全绿。
