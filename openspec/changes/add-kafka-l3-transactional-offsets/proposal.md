# Proposal: add-kafka-l3-transactional-offsets

## Why

Kafka 事务输出停在 L2：事务提交后、源 offset 提交前崩溃仍产生残留重复（PLANNING.md 第八节 P3 的 Kafka L3，前序 `add-sql-transactional-batch` 记录了延后理由：源 offset 与消费者组元数据在输入组件内，需要跨组件交接）。本变更通过进程内注册表完成交接，交付 L3。

## What Changes

1. **进程内组注册表**（`kafka_txn.rs`）：Kafka 输入在 `transactional_offsets: true` 时按 consumer group 注册——共享槽承载活消费者组元数据（`ConsumerGroupMetadata`，Arc 包裹非 Clone 类型）与订阅主题列表。
2. **输入侧**：`transactional_offsets: true` 时 connect 后发布组元数据；`KafkaAck::ack` 推进内存 frontier 但**跳过 store_offset**——broker 提交位只随输出事务前进，本地 store 可能越过将回滚的事务。
3. **输出侧**：`offset_commit_group: "<group>"`（要求 exactly_once，build 期校验）：`write_batch_transactional` 在 commit 前从各批次的 `__meta_partition`/`__meta_offset` 列推导覆盖位点（每分区取 max(offset+1)，librdkafka 的"下一位"约定），经注册表取组元数据，`send_offsets_to_transaction` 折入事务后 `commit_transaction`——输出原子性与位点推进同事务。
4. 主题路由：批次元数据只带分区不带主题；L3 经组注册表的主题列表路由（限定单主题输入，多主题显式报错）。
5. 无元数据列的批次贡献空位点（L3 只覆盖 Kafka→Kafka 流）；组无活输入（未声明 transactional_offsets）时显式报错。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `exactly-once-output`: 「L3 留 future」的边界兑现——Kafka 输出获得 offset-in-transaction 提交；诚实边界更新为"进程内输入/输出配对"（跨进程拆分部署不适用）。

## Impact

- 新增 `crates/arkflow-plugin/src/kafka_txn.rs`（组注册表）。
- `crates/arkflow-plugin/src/input/kafka.rs`：`transactional_offsets` 配置、组元数据发布、KafkaAck 跳过本地 store。
- `crates/arkflow-plugin/src/output/kafka.rs`：`offset_commit_group` 配置（+ schema + 校验）、`transactional_offsets_for_batches`、事务内 send_offsets。
- 测试：真实 broker 端到端（kafka_eos 第 5 例：L3 提交后同组新消费者零重投递）。

## Non-goals

跨进程输入/输出配对（分布式作业里输入与输出可能不在同节点——需把组元数据下沉到数据面协议，独立 change）；多主题输入的分区-主题归属（批次元数据缺主题维度）；SQL 类输出的事务位点（其输入位点模型不同）。
