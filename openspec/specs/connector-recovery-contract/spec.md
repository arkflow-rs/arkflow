# connector-recovery-contract Specification

## Purpose
TBD - created by archiving change fix-plugin-p2-batch. Update Purpose after archive.
## Requirements
### Requirement: input 连接器瞬时错误 SHALL 归类为 Disconnection
input 连接器在已连接状态下发生的瞬时失败（连接中断、IO 错误、订阅/读取超时）SHALL 以 `Error::Disconnection` 上报，使统一执行内核按既有 source 重连契约自动重连（connect + 退避）；数据毒丸类错误（协议反序列化失败、配置非法）SHALL 保持 `Error::Process` 上报，不得触发无限重连循环。`modbus` input 的四个读路径（coils/discrete_inputs/holding_registers/input_registers）与 `nats` input 的首次订阅/fetch 失败 SHALL 遵守本契约。

#### Scenario: modbus 连接中断后引擎重连
- **WHEN** modbus input 已连接后连接中断，一次 `read()` 返回错误
- **THEN** 该错误为 `Error::Disconnection`，内核进入 source 重连循环而非终止 source 任务

#### Scenario: nats 首次订阅失败可重连
- **WHEN** `nats` input（Regular 模式）connect 阶段首次订阅失败
- **THEN** 错误以 `Error::Disconnection` 上报，引擎重连重试

#### Scenario: 数据毒丸不触发重连
- **WHEN** 连接器收到无法反序列化的协议数据
- **THEN** 错误保持 `Error::Process` 上报，内核按致命错误处理

### Requirement: MQTT output 写路径 SHALL 有界重连且无泄漏
`mqtt` output SHALL 在 publish 失败或检测到 eventloop 已退出时执行惰性重连：先终止旧 eventloop 任务并尽力断开旧 client，再建立新 client 与新 eventloop，随后重试写入；重连 SHALL 有界（固定次数与退避），耗尽后返回错误交由上层语义处理。重连 SHALL NOT 泄漏旧 eventloop 任务或旧连接；`connected` 状态标志 SHALL 在 eventloop 退出时即失效，真实反映可用性。

#### Scenario: publish 失败后重连成功
- **WHEN** MQTT broker 短暂不可用导致一次 publish 失败，随后恢复
- **THEN** output 在有界重连内重建连接并完成该次写入，消息不丢

#### Scenario: 重连耗尽返回错误
- **WHEN** broker 持续不可用，重连次数耗尽
- **THEN** 本次 write 返回错误，不在组件内无限循环重试

#### Scenario: 重连不泄漏旧任务
- **WHEN** output 在已有 client/eventloop 的情况下再次建立连接
- **THEN** 旧 eventloop 任务被终止、旧连接被尽力断开后才替换，进程内无残留任务累积

#### Scenario: 状态标志反映真实连接
- **WHEN** eventloop 任务因错误退出而 close 未被调用
- **THEN** `connected` 标志已置为不可用，后续 write 触发重连而非误判已连接

### Requirement: 输出连接器按行目的地错位 SHALL 失败而不静默丢行

当输出连接器按行提取目的地(topic/subject/键)且该向量短于待发送 payload 数量时,连接器 SHALL 返回携带行数与目的地数量的命名错误,并保持交付的 acknowledgement 未结算。连接器 MUST NOT 静默跳过缺失目的地的行。

#### Scenario: MQTT per-row topic 短于 payload
- **WHEN** 一批消息经 codec 产生的 payload 数量多于按行提取的 topic 数量
- **THEN** write 返回 `Error::Process` 错误,错误信息同时包含 payload 数量与 topic 数量
- **THEN** 该交付的 acknowledgement 被 abort,source 位置不前进
- **THEN** 无任何 payload 被发布到 MQTT broker

#### Scenario: NATS per-row subject 短于 payload
- **WHEN** 一批消息的 payload 数量多于按行提取的 subject 数量
- **THEN** write 返回 `Error::Process` 错误,错误信息同时包含 payload 数量与 subject 数量
- **THEN** 无任何 payload 被发布到 NATS

### Requirement: 无界源的异常终止 SHALL 归类为 Disconnection 而非 EOF

订阅型(无界)输入连接器在底层流意外终止(订阅被关闭、流返回错误)时 SHALL 返回 `Error::Disconnection` 以触发内核重连;`Error::EOF` MUST 仅用于有界源的合法耗尽。把流错误映射为 `Error::EOF` 的连接器 MUST 将其改为 `Error::Disconnection` 或结构化错误。

#### Scenario: NATS 常规订阅被服务端关闭
- **WHEN** NATS 常规(非 JetStream)订阅的 message 流以 `None` 结束且源声明为无界
- **THEN** `read()` 返回 `Error::Disconnection`
- **THEN** 内核进入重连循环而不是向下游广播 EOS

#### Scenario: file 输入读取中途出错
- **WHEN** file 输入的 record-batch 流在耗尽之前返回错误(对象存储故障、格式损坏)
- **THEN** `read()` 返回 `Error::Disconnection`
- **THEN** 该错误不携带 `Error::EOF` 语义,已读前缀不作为完整数据集被确认

#### Scenario: sql 输入读取中途出错
- **WHEN** sql 输入的查询流在耗尽之前返回错误(连接断开、查询失败)
- **THEN** `read()` 返回 `Error::Disconnection`

#### Scenario: 有界源正常耗尽
- **WHEN** 有界源(file/sql/generate)的流返回 `None`
- **THEN** `read()` 返回 `Error::EOF`,内核按正常 EOS 传播

### Requirement: NATS JetStream 空闲拉取 SHALL 有界等待

JetStream 拉取循环在服务器返回零条消息时 SHALL 等待一个有界间隔(带 `expires` 的 pull 请求或固定退避)后重试,不得以无等待的 no-wait fetch 连续轮询。

#### Scenario: 空闲主题上的 JetStream 消费者
- **WHEN** JetStream 消费者在空闲主题上连续拉取且服务器立即返回零条消息
- **THEN** 相邻两次 fetch 之间存在非零等待(由 expires 或退避引入)
- **THEN** 一秒内的 fetch 请求数有上界而非数千次

