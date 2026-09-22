## Why

全工作区并行跑测试时两次观察到偶发失败：① `executor::remote::tests::disconnect_aborts_pending_and_closes_edge` 在 5s 预算内未观察到断连传播（pump 的 100ms flush tick 在数百线程争抢下延迟）；② `two_node_job_smoke` 的 30s 条件等待与无界 teardown await 在负载下被击穿/挂起，且后台任务在 runtime drop 中被轮询——一次「test result: ok 后进程以 101 退出、日志全绿」的幽灵失败即源于此。fleet_readiness 已用「有界等待 + abort()」的正确模式，smoke/node_resource_reporting 未对齐。

## What Changes

- `executor/remote.rs` 测试：13 处 5s 断连/abort 传播预算统一提升为 `TEST_PROPAGATION_BUDGET`（30s）——这些等待只观察正常 ~100ms 完成的条件，上限放宽零成本。
- `two_node_job_smoke.rs`：条件等待 30s → 60s；teardown 全部改为「有界等待 + 显式 abort()」（对齐 fleet_readiness 模式），并给「agent 必须停止」断言 15s 余量。
- `node_resource_reporting.rs`：无界 `await` teardown 改为有界等待 + abort()。
- `executor/tests.rs` tick 测试：读门 600ms → 1200ms——原 600ms 门与池内 400ms 延迟在负载下等比拉伸后空闲窗口消失（孤立复现率 ~1/5，孤立复现 6/6 绿），拉大余量修复。
- 无任何产品行为变更。

## 已知残留

- `disconnect_aborts_pending_and_closes_edge` 在三重并行全量下仍复现一次（30s 整预算耗尽、零 abort）：根因为 `PendingReceipts::abort_all` 经 `tokio::spawn` 设置 abort 标志，极端负载下 spawn 的任务可被长期饥饿——产品行为正确（abort_all 本身同步执行、真实断连时 flush 立即失败），属测试环境调度饥饿属性；预算已提升至 30s 并记录，后续若再复现可考虑把 abort 标志改为同步原子设置。

## Capabilities

### New Capabilities

<!-- 无新能力：纯测试稳定性加固。 -->

### Modified Capabilities

<!-- 无需求级变更。 -->

## Impact

- 仅测试文件与断言预算；生产代码零改动。

## Non-goals

- 不改产品代码的取消/关闭语义（pump 断连 abort 行为经评审为正确）。
- 不引入测试重试框架。
