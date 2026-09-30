## Why

v1.0 就绪度审查 P1-7 与 P1-11（`openspec/CODE_REVIEW_2026-09-29.md`）：Pulsar output 功能性损坏、input ack 活锁。证据（对照 pulsar 6.8.0 源码逐一坐实）：

- **P1-7 output 必崩**：`client.producer().build()` 从不 `with_topic`（`crates/arkflow-plugin/src/output/pulsar.rs:107-111`），pulsar 6.8.0 producer 构建在无 topic 时必然返回 `Error::Custom("topic not set")`（`pulsar-6.8.0/src/producer.rs:1062`）——`connect()` 永远失败，组件不可用。即便绕过：`send_messages_individually` 的 `_topics` 参数被完全忽略（`:171`），配置的 topic 表达式求值结果从未用于发送；`send_non_blocking(...).await` 的 `Ok(_)` 把返回的 `SendFuture` 丢弃（`:176`）——那只表示"已入队"，`write` 返回 Ok 不代表消息落 broker，fire-and-forget 冒充可靠投递。
- **P1-11 input ack 活锁**：消费任务在 `consumer.lock().await` 拿到互斥锁后跨越整个 `consumer.next().await` 等待期（`crates/arkflow-plugin/src/input/pulsar.rs:189-192`），而 `PulsarAck::ack` 需要同一把锁且 pulsar 6.8.0 的 `Consumer::ack` 要求 `&mut self`（`consumer/mod.rs:113`）——消息流停止时 `next()` 无限期持锁，已投递消息的 ack 永久排队，管线卡死。

本机 Docker 可用，参照 `kafka_eos.rs` 的 testcontainers 模式补真实 broker 端到端验证（审查指出"疑似从未在真实 Pulsar 端到端跑通"）。

## What Changes

- output：按 topic 缓存 producer（`with_topic(topic)` 构建后才可用），topic 表达式语义对齐 kafka output（`Scalar` → 全部消息同一 topic；`Vec` → 逐消息 `v[i]`，越界报配置错误而非 panic）；发送改为 `send_non_blocking(...).await?` 后 **await SendFuture 直至拿到 `CommandSendReceipt`**——write 返回 Ok 即表示 broker 已确认接收。
- input：消费循环的锁持有加上界——`next()` 用 `tokio::time::timeout` 包裹（锁内最长等待 `CONSUMER_LOCK_BOUND`），超时释放锁后重新进入 select（重查取消信号）。ack 最坏等待从"无限"变为一个有界值；消息持续流动时吞吐不受影响（next 立即返回）。
- 新增 `tests/pulsar_io.rs`：testcontainers 拉起 `apachepulsar/pulsar` standalone（参照 `kafka_eos.rs` 的 BrokerLease 卫生模式），覆盖——output 端到端投递并经真实 consumer 验收、broker 停止时 write 如实报错（可靠投递回归）、input 端到端消费 + ack 后同订阅不重投、**ack 活锁回归**（消息流停止后 ack 在有界时间内完成）。
- output：单条消息的回执等待加上界（`WRITE_RECEIPT_TIMEOUT` 30s）——实现中发现 pulsar 客户端失联时靠内部重连永不失败挂起中的回执，不加上界则 write 无限挂起而非报错。
- input：新订阅显式 `InitialPosition::Earliest`——实现中发现 pulsar-rust 默认 Latest，新订阅静默跳过积压（与 kafka input 默认 earliest 相悖，属数据丢失面）。
- 组件文档（en/zh）如实现与描述有出入则同步。

## Capabilities

### New Capabilities

- `pulsar-io`: Pulsar input/output 组件契约——可靠投递（回执级确认）、topic 表达式解析、ack 与消费互斥的有界性。

### Modified Capabilities

（无）

## Impact

- `crates/arkflow-plugin/src/output/pulsar.rs`：producer 缓存与可靠发送。
- `crates/arkflow-plugin/src/input/pulsar.rs`：消费循环锁上界。
- `crates/arkflow-plugin/tests/pulsar_io.rs`（新增）：testcontainers 端到端。
- 行为变化：output 的 write 现在等待 broker 回执（延迟升高、可靠性为真）；topic 为 Vec 且长度不足时从 panic 变为配置错误。

## Non-goals

- 不实现 output 断线重连/producer 重建策略（broker 故障经 `Connection` 错误上浮，由引擎处置；重连增强另行立项）。
- 不改 input 的订阅语义/重连结构（P1-11 只修锁的有界性）。
- 不做批量发送优化（send_all / batching）。
- 不引入 Pulsar 事务/exactly-once 语义。
- TLS/认证路径不做端到端覆盖（沿用既有校验逻辑）。
