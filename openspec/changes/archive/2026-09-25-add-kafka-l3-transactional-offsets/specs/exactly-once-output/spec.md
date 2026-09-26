# exactly-once-output 变更（Delta）

## ADDED Requirements

### Requirement: Kafka 输出 SHALL 支持事务内源位点提交（L3）

配置 `offset_commit_group`（要求 `exactly_once`）的 Kafka 输出 SHALL 在 `write_batch` 事务的 commit 前把覆盖的源位点折入同一事务：位点从各批次的 `__meta_partition`/`__meta_offset` 列推导（每分区取最大消费位点 +1），经进程内组注册表取得配对输入的消费者组元数据，调用 `send_offsets_to_transaction`。被指名的组 SHALL 有一个声明了 `transactional_offsets` 的同进程 Kafka 输入；该输入的 ack SHALL 推进内存 frontier 但不执行本地 `store_offset`。事务回滚时 broker 位点不得前进。

#### Scenario: L3 提交后无重投递

- **WHEN** 一条 Kafka→Kafka 消息经 L3 输出事务写出并提交
- **THEN** 同 consumer group 的新消费者不重投递该消息（位点已随事务提交）

#### Scenario: 事务回滚不推进位点

- **WHEN** 一个 L3 write_batch 在 commit 前失败回滚
- **THEN** broker 组位点不前进，恢复后从上一已提交事务之后重放

#### Scenario: 无配对输入显式失败

- **WHEN** `offset_commit_group` 指名的组在本进程无声明 `transactional_offsets` 的活输入
- **THEN** write_batch 以配置错误失败

#### Scenario: 非事务配置拒绝

- **WHEN** `offset_commit_group` 未搭配 `exactly_once`
- **THEN** build 期以配置错误拒绝

#### Scenario: 无元数据批次贡献空位点

- **WHEN** write_batch 的批次不含 Kafka 源元数据列
- **THEN** 事务不携带额外位点（L3 只覆盖 Kafka→Kafka 流），写入本身照常
