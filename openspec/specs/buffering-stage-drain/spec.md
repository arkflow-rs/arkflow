# Capability: 缓冲阶段排空

## Purpose

在消息被消费与被下发之间持有消息的组件（batch processor、memory buffer）的排空与失败路径契约：合并失败不丢已持有消息、EOS 与空闲超时排空、flush 与 close 语义分离。注意：统一内核下 stream 配置的 `buffer: {type: memory}` 编译为 no-op（有界通道已提供缓冲），本契约适用于通过插件注册表直接使用这些组件的场景；batch processor 的排空语义为生产路径。
## Requirements
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

### Requirement: batch 处理器合并 SHALL 经 schema 归一
`batch` 处理器 flush 时对多个输入批的合并 SHALL 经 schema 归一：字段取并集、缺失列以 null 填充、同名列类型冲突以显式错误失败；SHALL NOT 以任一批的 schema 做位置式拼接（位置式拼接在同类型异键序输入下静默错列、在列数变化下越界失败）。触发计数的 `count` SHALL 按合并前的**消息行数**计，而非输入批数。

#### Scenario: 异键序同类型行合并不错列

- **WHEN** 缓冲合并 `{"a":1,"b":2}` 与 `{"b":5,"a":6}`（同类型、键序不同）
- **THEN** 输出第二行 a=6、b=5（按列名对齐），不错列、不报错

#### Scenario: 后到的多字段行保留并集

- **WHEN** 缓冲合并 `{"a":1,"b":2}` 与 `{"a":3,"b":4,"c":5}`
- **THEN** 输出含 a/b/c 三列，首行 c 为 null

#### Scenario: 同名列类型冲突显式失败

- **WHEN** 缓冲合并的两批中同名列类型冲突（如 Int64 与 Utf8）
- **THEN** flush 以明确的合并错误失败（fail-closed），不做静默强转

#### Scenario: count 按行数触发

- **WHEN** `count: 3` 且上游一批含 2 行、下一批含 1 行
- **THEN** 累计 3 行即触发 flush（而非等满 3 个输入批）

