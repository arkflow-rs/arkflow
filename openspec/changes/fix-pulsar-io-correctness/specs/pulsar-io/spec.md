## ADDED Requirements

### Requirement: Pulsar output SHALL 在拿到 broker 回执后才报告成功

`pulsar` output 的 `connect()` SHALL 为实际使用的 topic 构建 producer（构建期必须携带 topic），并按 topic 缓存复用。topic 表达式解析 SHALL 对齐 kafka output 语义：`Scalar` 表示全部消息发往同一 topic，`Vec` 表示逐消息取 `v[i]`，长度不足时 SHALL 返回配置错误而非 panic。`write()` SHALL 等待每条消息的发送回执（`CommandSendReceipt`）后才返回 `Ok`——broker 不可达或拒绝时 SHALL 返回错误。

#### Scenario: 端到端投递被真实 consumer 验收

- **WHEN** output 经 `connect()` + `write()` 发送 N 条消息到真实 Pulsar topic，随后以独立 consumer 消费该 topic
- **THEN** consumer 收到全部 N 条且内容一致，`write()` 此前已因回执确认返回 `Ok`

#### Scenario: broker 停止时 write 有界报错

- **WHEN** broker 在 `connect()` 成功后停止，随后调用 `write()`
- **THEN** `write()` 在有界时间内返回错误——既不以 fire-and-forget 方式返回 `Ok`，也不无限挂起（pulsar 客户端内部重连不会主动失败挂起中的回执，组件必须自带回执等待上界）

#### Scenario: value_field 指定载荷列

- **WHEN** 配置了 `value_field` 且批次中存在该名字的二进制或字符串列
- **THEN** 每行以该列的值作为消息载荷发送（优先于 codec 编码）；列缺失或类型不支持时返回明确的配置错误

#### Scenario: Vec topic 长度不足报配置错误

- **WHEN** topic 表达式求值为 `Vec` 且长度小于消息数
- **THEN** 返回明确的配置错误，不 panic

### Requirement: Pulsar input 的 ack SHALL 不被消费等待无限阻塞

`pulsar` input 的消费任务与 ack 共享同一 consumer 互斥锁（pulsar 6.x `Consumer::ack` 要求 `&mut self`）：消费循环对 `next()` 的等待 SHALL 有上界（超时释放锁、重新检查取消信号后再继续），消息流停止时 ack 的等待 SHALL 有界完成。消息持续流动时 SHALL 不引入额外延迟（`next()` 就绪即返回）。

#### Scenario: 消息流停止后 ack 有界完成

- **WHEN** 一条消息已被 `read()` 交付且 broker 不再投递新消息（消费任务回到 `next()` 等待）
- **THEN** 该消息的 `ack()` 在有界时间内完成，管线不卡死

#### Scenario: ack 后同订阅不重投

- **WHEN** 消息被成功 ack 后，以相同 subscription 创建新的消费者
- **THEN** 已 ack 的消息不被重复投递

### Requirement: Pulsar input 的新订阅 SHALL 从最早消息开始

`pulsar` input 创建消费者时 SHALL 显式指定 `InitialPosition::Earliest`（pulsar 客户端默认 Latest）：新订阅不得静默跳过既有积压，与 kafka input 默认从最早消费的语义对齐。

#### Scenario: 积压消息不被跳过

- **WHEN** 消息已写入 topic 且 input 以全新订阅连接
- **THEN** 既有积压消息按序被读取，不丢失
