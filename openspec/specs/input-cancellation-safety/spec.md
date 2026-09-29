# Capability: Input Cancellation Safety

## Purpose

通道型（channel-based）input 必须遵守引擎 `Input::read` 的取消安全契约（`arkflow-core`）。本能力固定单一 await 点的 read 形态、生产者侧（后台接收任务内）的 codec 解码，以及保障该契约的回归测试框架。

## Requirements

### Requirement: 通道型 Input 的 read SHALL 取消安全

随引擎发行的通道型 input（MQTT、Pulsar、NATS、Redis、WebSocket、HTTP）的 `Input::read` SHALL 满足引擎的取消安全契约（`arkflow-core` `Input::read` 文档）：在数据认领（从内部通道弹出）与返回之间 SHALL NOT 存在可被取消的 await 点——认领的投递以解码完成的 `(MessageBatchRef, Arc<dyn Ack>)` 形态驻留通道，`read()` 对数据的等待 SHALL 是单一名额的 await，其返回即投递完成。codec 解码与 ack 构造 SHALL 发生在 connector 的后台接收任务内（认领之前）；解码失败 SHALL 经通道以错误形态传递，`read()` 原样上抛。控制帧/非数据事件（如 WebSocket Ping/Pong）SHALL 在后台任务内过滤，`read()` 不得以递归重读跳过它们。

#### Scenario: 解码进行中的取消不丢消息

- **WHEN** 引擎源链在 `read()` pending 时因 idle tick/barrier/取消丢弃该 future，而此刻该投递的 codec 解码尚未完成（无论解码发生在 read 内还是后台任务内）
- **THEN** 该投递不丢失：随后重新发起的 `read()` 恰好一次地取回同一条消息及其 ack

#### Scenario: 解码失败语义保持

- **WHEN** 一条消息的 codec 解码在后台任务中失败
- **THEN** `read()` 返回该解码错误（引擎进入既有失败路径），且该消息不进入数据面

### Requirement: 取消安全 SHALL 有回归测试防线

插件层 SHALL 提供可复用的契约测试框架：一个门控 codec（可确定性阻塞解码以制造"解码进行中"窗口）与引擎形态的取消循环（丢弃 pending 的 read future 后重试）；随引擎发行的通道型 input 中，可在进程内驱动的（WebSocket、HTTP）SHALL 有使用该框架的端到端回归测试，其余（MQTT、Pulsar、NATS、Redis，需外部 broker）SHALL 由共享投递模式的单元断言覆盖，并在具备 broker 测试基建后补端到端用例。

#### Scenario: 框架能抓到认领后解码的违例实现

- **WHEN** 一个"先弹出通道、后在 read 内 await 解码"的故意违例实现接受框架的取消循环测试
- **THEN** 测试失败（消息丢失被检出）——框架本身被证明有效

#### Scenario: 合规实现零误报

- **WHEN** 一个单数据 await 点的合规实现（如 HTTP input）接受同一框架测试
- **THEN** 测试通过，消息恰好一次送达
