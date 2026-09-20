## MODIFIED Requirements

### Requirement: 远程边背压有界

跨节点边 SHALL 端到端保持有界缓冲：下游消费停滞时，缓冲压力 SHALL 沿 TCP 反压至上游 chain 的发送点，系统 MUST NOT 无界缓存 Envelope、accepted connections、receipt controls 或 pending acknowledgements。所有由远端声明的 frame 长度 SHALL 在分配前通过配置上限校验。

#### Scenario: 慢消费者反压传播

- **WHEN** 下游 subtask 停止消费且本地入站通道写满
- **THEN** 入站 pump 停止读取 socket，TCP 窗口收缩，上游 chain 的发送调用阻塞而非内存增长

#### Scenario: accepted connection queue reaches its bound

- **WHEN** 新连接到达且已有连接数或 accepted queue 达到配置上限
- **THEN** 新连接被关闭并记录 bounded-resource failure，现有连接和已接收 Envelope 不受影响

### Requirement: 远程边失败 fail-closed

远程边连接断开、协议校验失败、认证失败、资源上限溢出或对端不可达时，内核 SHALL 使受影响的 Job attempt 失败并清理该边两端资源，MUST NOT 让链路停留在半开状态继续产出或丢弃数据。透明 reconnect 不属于本版本语义。

#### Scenario: 断链失败传播

- **WHEN** 一条已建立的远程边 TCP 连接中断
- **THEN** 双方对应链路以错误终止（同一 TCP 连接双向同时失败），attempt 进入失败上报，由既有 generation fencing 重新放置

#### Scenario: 非法或未认证帧到达

- **WHEN** 已连接的 peer 发送非法帧、未认证帧或不属于当前 quad/generation 的帧
- **THEN** 连接被关闭、pending ack 被终止、attempt 进入 fail-closed 路径且不交付该帧

#### Scenario: kernel 取消清理连接

- **WHEN** Job kernel 因重新放置或停止被取消
- **THEN** 其建立的远程边出站连接与入站 listener 会话全部关闭，无残留 socket、registry 与悬持 ack

### Requirement: 线协议帧编解码

跨节点传输 SHALL 使用定长帧头（src/dst 四元组、长度、类型）+ Arrow IPC 数据负载 + serde 控制负载的线格式；数据帧 MUST 能在接收端还原为等值 RecordBatch（含字典编码列）。接收端 SHALL 在任何 Flatbuffer 或 Arrow body 切片前验证 piece、消息和 body 的边界。

#### Scenario: 数据帧往返等值

- **WHEN** 一个含字典编码字符串列的 RecordBatch 经帧编码、TCP 传输、帧解码
- **THEN** 接收端还原出与发送端等值的 RecordBatch

#### Scenario: 非法帧头拒绝

- **WHEN** 接收端读到未知帧类型或长度超出上限的帧头
- **THEN** 连接以协议错误关闭，不尝试继续解析后续字节

#### Scenario: 声明的 Arrow body 超出 piece

- **WHEN** IPC 消息的 bodyLength 大于 piece 中实际剩余字节数
- **THEN** 解码返回错误并关闭连接，而不是触发越界 panic
