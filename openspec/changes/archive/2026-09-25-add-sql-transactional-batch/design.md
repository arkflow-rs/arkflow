# Design: add-sql-transactional-batch

## 决策

1. **事务粒度 = ack 区间**：与 `exactly-once-output` 的「one write_batch per ack range」不变量对齐——事务边界即重放边界。
2. **sqlx 裸连接事务**：该输出用 sqlx MySqlConnection/PgConnection（非 sea-orm）；`Connection::begin` → 逐批 `execute(&mut *txn)` → `commit`。失败路径 drop txn = 自动回滚。
3. **codec/upsert 校验在事务外**：失败快速返回，不占用数据库事务持有时间。
4. **锁顺序不变**：沿用既有 `conn_lock`，事务在同一锁临界区内完成，无新增死锁面。

## 风险

- 长批次持锁时间变长（事务跨全部批次）——与逐条 autocommit 相比锁窗口更长；批量场景（窗口发射）本就是单 write_batch，实际影响限于多消息聚合路径。
