# buffering-stage-drain 增量

## MODIFIED Requirements

### Requirement: 缓冲组件失败路径 SHALL NOT 丢弃已持有的消息

在消息被消费（从输入侧取出）与被下发（交给下游）之间持有消息的组件，其内部合并/转换失败时 SHALL 保留已持有的消息与其 ack 待重试，不得丢弃：memory buffer 的 `read()` 侧合并（`concat_batches`）失败时队列内容 SHALL 原样保留并返回错误，后续 `read()` SHALL 能重新尝试同一批消息；batch processor 的 `flush()` 合并失败时缓冲 SHALL 保留已积累的消息；window buffer 家族（tumbling/sliding/session 共用的 `process_window`）合并或归一失败时，已取出的各 input 队列 SHALL 原样放回（ack 不 settle 也不 abort，随队列保留待重试）并返回错误。

#### Scenario: memory buffer 合并失败后队列保持可重试

- **WHEN** memory buffer 队列中存在多条待合并消息，且合并因 schema 不兼容等原因失败
- **THEN** 本次 `read()` 返回错误且队列内容原样保留（条数与顺序不变），随后再次 `read()` 时同一批消息仍可被取出处理

#### Scenario: batch processor 合并失败时缓冲保留

- **WHEN** batch processor 触发 flush 且批次合并失败
- **THEN** 已积累在缓冲中的消息不被清除，错误向上传播，后续处理可再次尝试合并

#### Scenario: window buffer 合并失败队列与 ack 保留

- **WHEN** window buffer 各 input 队列持有消息且合并（含归一）失败
- **THEN** 已取出的队列内容原样放回（条数、顺序与待结算 ack 不变），`read()` 返回错误，后续 `read()` 可重新处理同一批消息

## ADDED Requirements

### Requirement: window buffer 跨 input 异构 schema SHALL 归一合并
window buffer 汇集多个 input 的批次时，各 input schema 不完全一致（缺列）SHALL 归一到并集 schema（缺失列以 null 填充）后合并产出，不得因 schema 不一致直接失败；同名同列类型冲突 SHALL 返回指明列名与两个类型的错误（走失败保留路径，不丢数据）。

#### Scenario: 缺列归一合并
- **WHEN** input A 的批次含列 `id`/`value`，input B 的批次含列 `id`（缺 `value`）
- **THEN** 产出并集 schema（`id`/`value`）的批次，B 的行 `value` 为 null，合并不报错

#### Scenario: 类型冲突报错不丢数据
- **WHEN** 两个 input 的同名列类型不同（如 Utf8 与 Int64）
- **THEN** 返回含列名与两个类型的错误，队列与 ack 按失败保留路径原样放回

### Requirement: window buffer 读者 SHALL 被关闭与写入可靠唤醒
window buffer 的读者等待 SHALL 同时监听唤醒通知与关闭 token（`select`），`close()`/`flush()` 触发时即使唤醒通知与检查之间存在竞态、即使队列为空，读者 SHALL 在有限时间内被 token 唤醒并按 close 语义（排空余量后结束）返回，MUST NOT 因错过最后一次唤醒通知而永久挂起。

#### Scenario: close 时空队列读者不挂起
- **WHEN** 读者正在等待新消息、队列为空且唤醒通知恰在读者检查后触发，随后 `close()` 被调用
- **THEN** 读者被关闭 token 唤醒并在有限时间内返回结束，不永久阻塞

#### Scenario: 正常数据路径唤醒不受影响
- **WHEN** 读者等待期间上游写入消息
- **THEN** 读者按既有节奏被唤醒并处理消息
