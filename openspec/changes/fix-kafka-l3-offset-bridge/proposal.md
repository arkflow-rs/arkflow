# Proposal: fix-kafka-l3-offset-bridge

## Why

L3（事务内源位点提交）桥有四个独立缺陷，共同后果是 EOS 声明不可信：

1. **offset 发送错误未捕获**：`crates/arkflow-plugin/src/output/kafka.rs:457-471` 在 `spawn_blocking` 里调用 `send_offsets_to_transaction` 后只检查 join 结果——rdkafka 该调用经 delivery 回调**异步**报错（如 producer 被 fence、coordinator 错误），当前未捕获、commit 前不检查。失败模式下数据提交而位点未提交，EOS 静默降级为 at-least-once。
2. **位点取批内最大值而非已结算前沿**：`transactional_offsets_for_batches`（`output/kafka.rs` 的推导 + 调用点 `:446-471`）按本批出现的行取每分区 `max(row_offset)+1`。内容路由/扇出/过滤拓扑中，同分区更早的位点不在本批——事务把组游标**跳过**它们，崩溃后这些记录永不重投（**丢数据**；单线性流安全）。
3. **配对缺失 fail-open**：输入侧 `transactional_offsets: true` 从第一次 ack 起抑制 `store_offset`（`input/kafka.rs:122-129, 938-946`），但无任何构建期校验保证存在指名该组的 `offset_commit_group` 输出（现有守卫只在输出侧 write 时检查反向）。错配时组位点**永远**无人提交，崩溃后全凭 `auto.offset.reset`（`latest` 即全跳过）。
4. **undo 违反"只有事务移动该组"**：`KafkaAck::undo` 在 `transactional_offsets` 下仍调 `store_offset`（`input/kafka.rs:1042-1049`）——本地回退可被周期 auto-commit 发布，破坏 L3 不变式。

## What Changes

- **异步错误捕获**：producer 的 delivery 回调记录事务性错误（含 `send_offsets_to_transaction` 的异步失败），`commit_transaction` 前检查；失败 → abort 事务、批返回错误、ack 扣住 → 重放。
- **位点推导钳制到配对输入的连续前沿**：组注册表（`kafka_txn`）暴露配对输入的进程内 `CommitFrontier` 快照；每分区提交 `min(批内最大 next, 前沿 next)`，进程内单调不回退。未结算位点永不进事务——无跳过、无丢失；滞后部分由后续事务收敛（尾部至多一批重复，at-least-once 下界）。
- **配对启动校验**：流/作业组件全部构建完成后（connect 阶段）校验——声明 `transactional_offsets` 的输入必有指名其组的 `offset_commit_group` 输出，缺失即启动失败（fail-closed）。
- **undo 修正**：`transactional_offsets` 下 `KafkaAck::undo` 不调 `store_offset`（组位点只经事务移动；内存 frontier 回退已足够保证重投）。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `exactly-once-output`: MODIFIED "Kafka 输出 SHALL 支持事务内源位点提交（L3）"——位点推导钳制前沿、异步错误 fail-closed、启动期配对校验、undo 不触 store_offset。

## Non-goals

- 不实现逐 offset 的跨事务覆盖账本（真·多分支 EOS 需要它；本期以"前沿钳制 + 尾部重复"收口丢失方向，重复方向维持 at-least-once 声明）。
- 不改 L2（纯事务输出无位点折入）与本地 `store_offset` 路径。
- 不引入跨进程的组注册表（单进程配对语义维持）。
- 不改 `auto.offset.reset` 相关文档语义。

## Impact

- `crates/arkflow-plugin/src/output/kafka.rs`（delivery 回调错误捕获、位点钳制、commit 前检查）。
- `crates/arkflow-plugin/src/kafka_txn.rs`（组注册表暴露前沿快照）。
- `crates/arkflow-plugin/src/input/kafka.rs`（undo 分支；注册前沿引用）。
- connect 阶段校验挂点（`arkflow-core` resource_guard 或 stream/job 构建后校验段）。
- 测试：`kafka_eos.rs`（testcontainers EOS 套件）扩充四类场景；组注册表单测。
- 文档：exactly-once/distributed 页（en/zh）说明前沿钳制语义与尾部重复界。
