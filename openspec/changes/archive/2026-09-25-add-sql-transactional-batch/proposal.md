# Proposal: add-sql-transactional-batch

## Why

PLANNING.md 第八节 P3（eos-l3-and-transactional-sinks）的可独立交付增量：SQL output 的 `write` 单条 INSERT 本身原子，但 trait 默认的 `write_batch` 逐条调用 `write`——一个 ack 区间跨多条消息时，中途失败留下部分写入窗口，恢复重放后表现为可见重复/部分行。Kafka L3（源 offset 进 producer 事务）经评估需要输入侧消费者组元数据经 ack 链交接给输出（跨组件协议改造），明确延后并记录。

## What Changes

1. SQL output 覆写 `write_batch`：整个 ack 区间在一个 `BEGIN…COMMIT` 事务内执行（MySQL/Postgres 双方言），任一批次失败全部回滚——部分写入窗口关闭。
2. 逐条 `write` 路径保持不变（单语句原子，行为零变化）。
3. upsert 校验等前置检查移入事务前（快速失败不占事务）。
4. Kafka L3 延后决策记录于 exactly-once-output 规格（Non-goal 明示架构原因）。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `sql-output`: `write_batch` 升级为事务原子（一个 ack 区间一个事务）。
- `exactly-once-output`: L3 边界更新——Kafka offset-in-transaction 延后，理由（输入侧 cgm 交接）入档。

## Impact

- `crates/arkflow-plugin/src/output/sql.rs`：`write_batch` 覆写 + `execute_insert_transactional`（sqlx 事务，双方言）。
- 离线测试无法覆盖真实回滚（需活库）；既有 SQL 文本断言与 #[ignore] 活库测试回归通过。

## Non-goals

Kafka `send_offsets_to_transaction`（L3）——需要 kafka input 的 ConsumerGroupMetadata 与待提交 offset 经 ack 协议传递给 output（输入/输出耦合的协议改造），单独立项；只读副本上的事务隔离级别调优。
