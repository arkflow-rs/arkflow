---
sidebar_position: 6
---

# 精确一次投递

ArkFlow 默认提供**至少一次**投递。恢复时,在途消息会被重放,并可能多次投递给输出。对于不能容忍重复的 sink,ArkFlow 可通过事务性(transactional)Kafka 输出以可选项提供**精确一次(exactly-once)**投递(更准确地说,是*实际一次(effectively-once)*)。本页说明它保证什么、如何配置,以及边界在哪里。

## 工作原理

为精确一次配置的 Kafka 输出使用**事务性生产者**:

- `init_transactions` 在 `connect()` 时调用一次;
- 在每个待确认的批次之前,输出开启一个事务,发送批次中的每条消息,然后提交;
- WAL 游标前进(且数据源被提交)只在事务成功提交之后发生。

工作单元是**一个 ack 范围 = 一次 `write_batch` 调用 = 一个 Kafka 事务**。如果缓冲(内存、滚动/滑动/会话窗口,或 join)把若干输入消息聚合为一个输出批次,整个批次就是一个原子事务单元。以 `isolation.level=read_committed` 读取的下游消费者会原子地观察到每个批次——**要么全部消息,要么全无**。

任何失败时(需要中止的 `commit_transaction` 错误,或崩溃),该批次**不被确认**,WAL 游标**不**前进,该范围在恢复时重放——这将开启一个新的事务。

## 配置

在 Kafka 输出上用两个键启用精确一次:

```yaml validate=fragment wrap=output
output:
  type: kafka
  brokers:
    - localhost:9092
  topic:
    type: value
    value: orders-copy
  exactly_once: true
  transactional_id: arkflow-orders-copy-0   # stable across restarts; unique per producer
```

- **`exactly_once: true`** 开启事务性生产。
- **`transactional_id`** 在启用 `exactly_once` 时**必填**。它必须**跨重启稳定**(由你负责)且**每条流的生产者唯一**。重启时 broker 用同一个 id 对先前的生产者 epoch 做隔离(fencing),并中止其在途(僵尸)事务,因此僵尸写入永远不会对 `read_committed` 消费者可见。

精确一次叠加在至少一次摄取之上,因此输入侧需要启用持久化,使读取与输出之间的崩溃不丢失数据:

```yaml validate=fragment wrap=durability
durability:
  enabled: true
  path: "./data/wal-eos"
  sync: group_commit
```

完整可运行示例见
[`examples/eos-kafka.yaml`](https://github.com/arkflow-rs/arkflow/blob/main/examples/eos-kafka.yaml)
(Kafka → Kafka,消费-变换-生产)。

## 真实的边界(请务必阅读)

Kafka 事务性输出消除了两类特定的重复来源:

- **事务内部分写入** —— 事务原子提交,因此 `read_committed` 消费者永远不会看到部分批次;
- **僵尸生产者重复** —— 稳定的 `transactional_id` 会在重启间隔离(fence)掉过期的生产者 epoch。

当崩溃发生在**生产者事务已提交之后、源偏移量提交之前**时,它**并不**保证没有重复。源偏移量是异步提交的(受数据源自动提交间隔限制,如 Kafka 默认的 5 秒)。如果进程在这个窗口内崩溃,恢复时数据源会重新投递该范围,新的生产者再写一次,`read_committed` 下游消费者就会观察到重复行。

**这类残留重复必须在下游吸收**——通过去重键、业务级幂等,或幂等 sink(如 UPSERT)。请据此设计你的下游消费者。

真正端到端的精确一次——通过 `send_offsets_to_transaction` 在生产者事务*内部*提交源偏移量(仅限 Kafka → Kafka)——属于**未来工作(L3)**,目前不提供。

## 要求摘要

- `exactly_once: true` 要求非空的 `transactional_id`;否则校验失败并给出明确错误。
- WAL 的对象存储 `node_id` 与 Kafka `transactional_id` 是**相互独立**的配置值——互不派生。
- 事务性 Kafka 输出之外的其他输出保持当前默认的至少一次行为。其他 sink 的幂等适配器(SQL UPSERT 等)可在后续变更中添加。
