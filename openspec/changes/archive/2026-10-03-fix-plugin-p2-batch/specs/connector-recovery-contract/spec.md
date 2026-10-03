# connector-recovery-contract 增量

## ADDED Requirements

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
