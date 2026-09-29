## MODIFIED Requirements

### Requirement: Kafka 输出 SHALL 支持事务内源位点提交（L3）

配置 `offset_commit_group`（要求 `exactly_once`）的 Kafka 输出 SHALL 在 `write_batch` 事务的 commit 前把覆盖的源位点折入同一事务：位点从各批次的 `__meta_partition`/`__meta_offset` 列推导（每分区取最大消费位点 +1），经进程内组注册表取得配对输入的消费者组元数据，调用 `send_offsets_to_transaction`。被指名的组 SHALL 有一个声明了 `transactional_offsets` 的同进程 Kafka 输入；该输入的 ack SHALL 推进内存 frontier 但不执行本地 `store_offset`。事务回滚时 broker 位点不得前进。

位点推导 SHALL 逐行校验来源 topic：批次携带行级 topic 元数据（`__meta_ext` map 的 `topic` 键，由 Kafka input 逐消息产出）时，任何携带 Kafka 位点元数据（`__meta_partition`/`__meta_offset` 非空）的行其 topic SHALL 等于配对输入订阅的 group topic；不满足时 `write_batch` SHALL 以显式数据错误失败（fail-closed：事务不提交、位点不推进），MUST NOT 把异源行的位点折入 group topic（否则该分区位点被事务性跳前、中间记录静默丢失）。批次整体不携带 `__meta_ext` 列时维持既有推导（兼容旧元数据形态，partition/offset 列同缺时沿用"无元数据批次贡献空位点"）。

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

#### Scenario: 混入异源 topic 的行显式失败

- **WHEN** L3 write_batch 的批次携带 `__meta_topic` 列，且存在 Kafka 位点元数据的行其 topic 不等于配对输入订阅的 group topic（多源/扇入图混入的另一个 Kafka 源）
- **THEN** write_batch 以明确的错误失败，事务不提交、位点不推进——不得把这些行的位点折入 group topic 导致中间记录被跳过

声明 `transactional_offsets` 的输入不得进入分区指派（assign）模式：显式指派的消费者不加入组、无法提供组元数据，`assign_partition` SHALL 以明确配置错误拒绝该组合。

#### Scenario: 分区指派与 L3 组合被拒绝

- **WHEN** 一个声明 `transactional_offsets` 的 Kafka 输入被图构建期分区指派
- **THEN** assign_partition 以明确的配置错误失败，指明 L3 需要 subscribe 模式的单读者输入
