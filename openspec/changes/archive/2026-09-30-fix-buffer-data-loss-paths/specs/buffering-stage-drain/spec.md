## ADDED Requirements

### Requirement: 缓冲组件失败路径 SHALL NOT 丢弃已持有的消息

在消息被消费（从输入侧取出）与被下发（交给下游）之间持有消息的组件，其内部合并/转换失败时 SHALL 保留已持有的消息与其 ack 待重试，不得丢弃：memory buffer 的 `read()` 侧合并（`concat_batches`）失败时队列内容 SHALL 原样保留并返回错误，后续 `read()` SHALL 能重新尝试同一批消息；batch processor 的 `flush()` 合并失败时缓冲 SHALL 保留已积累的消息。

#### Scenario: memory buffer 合并失败后队列保持可重试

- **WHEN** memory buffer 队列中存在多条待合并消息，且合并因 schema 不兼容等原因失败
- **THEN** 本次 `read()` 返回错误且队列内容原样保留（条数与顺序不变），随后再次 `read()` 时同一批消息仍可被取出处理

#### Scenario: batch processor 合并失败时缓冲保留

- **WHEN** batch processor 触发 flush 且批次合并失败
- **THEN** 已积累在缓冲中的消息不被清除，错误向上传播，后续处理可再次尝试合并

### Requirement: 缓冲数据 SHALL 在 EOS 与空闲超时排空

batch processor SHALL 在上游有界源到达 EOS 时通过 `finish()` 排空不足 `count` 的部分批次并正常下发（ack 按常规输出路径结算）；SHALL 在输入空闲期间通过 `on_tick()` 于 `timeout_ms` 到期时刷新部分批次，不依赖新消息到达触发。`close()` SHALL 仅做资源释放，不得主动丢弃未下发数据；异常终止路径（如取消）下缓冲仍有残留时 SHALL 记录包含条数的告警日志。

#### Scenario: EOS 排空部分批次

- **WHEN** 上游有界源到达 EOS，batch processor 缓冲中持有不足 `count` 的部分批次
- **THEN** `finish()` 返回合并后的批次并下发下游，消息不丢失，对应 ack 按常规输出路径结算

#### Scenario: 流停顿时按超时刷新

- **WHEN** batch processor 缓冲非空且超过 `timeout_ms` 无新消息到达
- **THEN** 空闲 tick 触发的刷新把部分批次下发，无需等待下一条消息到达

#### Scenario: 关闭不产生静默丢弃

- **WHEN** 链路退出且 batch processor 缓冲在 `finish()` 之后仍有残留（仅取消等异常路径可达）
- **THEN** `close()` 释放资源并记录包含残留条数的 warn 日志，不静默丢弃

### Requirement: memory buffer 的 flush 与 close SHALL 语义分离

`flush()` SHALL 为非终止操作：仅唤醒读者排空当前累积，超时释放的后台任务 SHALL 继续运行；`close()` SHALL 为终止操作。`close()` 之后 `read()` SHALL 先排空队列余量再返回结束标志，不得因关闭而丢弃余量。

#### Scenario: flush 之后超时释放仍然工作

- **WHEN** `flush()` 被调用排空累积后，再次写入消息并等待超过 `timeout`
- **THEN** 超时释放仍然生效，消息可被 `read()` 取出

#### Scenario: close 后排空余量再结束

- **WHEN** `close()` 被调用时队列中仍有未读消息
- **THEN** 后续 `read()` 先返回余量消息，队列清空后返回结束标志（None）
