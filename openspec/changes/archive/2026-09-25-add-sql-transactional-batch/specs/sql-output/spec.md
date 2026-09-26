# sql-output 变更（Delta）

## ADDED Requirements

### Requirement: write_batch SHALL be transactional

一次 `write_batch` 调用（一个 ack 区间）SHALL 在单个数据库事务内执行全部批次的写入：任一批次失败，整个事务回滚，不留下部分写入；调用方向上层返回错误使 ack 不推进、恢复时整个区间重放。单条 `write` 路径（单语句）行为不变。upsert 键校验 SHALL 在事务开始前完成。

#### Scenario: 批次中途失败整体回滚

- **WHEN** 一个 write_batch 区间含三个批次且第二个执行失败
- **THEN** 第一个批次的写入被回滚，数据库不包含该区间的任何行，ack 不推进

#### Scenario: 单条写入行为不变

- **WHEN** 输出以逐条 `write` 使用（无 write_batch 聚合）
- **THEN** 每条写入仍是单语句原子提交，行为与现状一致
