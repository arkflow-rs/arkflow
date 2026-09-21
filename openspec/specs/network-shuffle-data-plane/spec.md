# network-shuffle-data-plane Specification

## Purpose
Cross-node execution-edge transport for the unified execution kernel: wire frames, the network manager, remote edge endpoints, bounded backpressure, the Ack three-state mirror-receipt protocol, and fail-closed semantics for split placement. Synced from change add-network-shuffle-data-plane.

## Requirements
### Requirement: 远程边与本地边语义一致
执行内核中跨节点的边 SHALL 以与进程内有界通道不可区分的语义传输 `Envelope`：同通道严格 FIFO、控制元素（Barrier/Watermark/Eos）与数据交错保序、通道容量有界。控制元素 SHALL 广播到该边的全部下游 subtask；数据元素 SHALL 按 key-group 路由到单一目标 subtask。

#### Scenario: barrier 跨远程边保持相对数据的位置
- **WHEN** 上游 chain 依次发出 Data(b1)、Barrier(c5)、Data(b2)，且边跨节点
- **THEN** 下游 chain 依相同顺序收到 Data(b1)、Barrier(c5)、Data(b2)

#### Scenario: 控制元素到达全部副本
- **WHEN** 一条 partitioned 远程边 fan-out 到 3 个下游 subtask，上游发出 Barrier(c5)
- **THEN** 3 个下游 subtask 所在的每个连接都各收到一份 Barrier(c5)，无副本被 key 路由遗漏

#### Scenario: 多输入顶点跨远程边对齐
- **WHEN** 一个 chain 有两条入边（一本地一远程），barrier c5 先到达本地边
- **THEN** 下游按既有 `Aligner` 语义缓冲远程边数据，直到 c5 到达后快照并释放，对齐逻辑不感知边是否远程

### Requirement: 远程边背压有界
跨节点边 SHALL 端到端保持有界缓冲：下游消费停滞时，缓冲压力 SHALL 沿 TCP 反压至上游 chain 的发送点，系统 MUST NOT 无界缓存 Envelope、accepted connections、receipt controls 或 pending acknowledgements。所有由远端声明的 frame 长度 SHALL 在分配前通过配置上限校验。

#### Scenario: 慢消费者反压传播

- **WHEN** 下游 subtask 停止消费且本地入站通道写满
- **THEN** 入站 pump 停止读取 socket，TCP 窗口收缩，上游 chain 的发送调用阻塞而非内存增长

#### Scenario: accepted connection queue reaches its bound

- **WHEN** 新连接到达且已有连接数或 accepted queue 达到配置上限
- **THEN** 新连接被关闭并记录 bounded-resource failure，现有连接和已接收 Envelope 不受影响

### Requirement: Ack 三态镜像回执
数据 Envelope 跨远程边传输时，其源 ack SHALL 延迟到**所有**下游副本的回执齐备后才完成；下游的持有语义 SHALL 忠实镜像回上游。上游聚合 SHALL 复用既有 fan-out 分支 ack 机制（分支计数、防重复、abort/undo 补偿），不得引入第二套聚合状态机。

#### Scenario: 全副本回执后源 ack 完成
- **WHEN** 一条远程边 fan-out 到 2 个下游 subtask，两个副本各自完成本地处理后回发 Acked(seq)
- **THEN** 上游在收到两个 Acked(seq) 后才完成对应 fan-out 分支，源偏移随之推进

#### Scenario: 窗口持有排除出 drain 等待
- **WHEN** 下游因事件时间窗口持有某批次并回发 Held(seq)
- **THEN** 上游对该 ack 调用 mark_held，源链的 barrier drain 等待不包含它；窗口触发后下游回发 Released(seq)，上游调用 release_held 使其回到在途集合

#### Scenario: Held 控制帧丢失安全降级
- **WHEN** 下游发出的 Held(seq) 因同步投递路径失败而丢失
- **THEN** 上游该 ack 保持在 drain 等待集合，本轮 barrier drain 超时后 checkpoint fail-closed，不产生数据丢失或偏移错推

#### Scenario: 下游崩溃不产生已 ack 丢失
- **WHEN** 下游节点在处理完批次但回执发出前崩溃，或回执在网络中丢失
- **THEN** 上游对应 ack 永不完成，barrier drain 超时使本轮 checkpoint 失败（fail-closed），恢复后从上个 sealed cut 重放（at-least-once）

#### Scenario: 下游处理失败靠超时发现
- **WHEN** 下游 chain 处理某批次失败（非连接断开），失败走下游本地 error 上报
- **THEN** 上游对应分支在 barrier drain 超时前保持未完成，超时后本轮 checkpoint 失败（fail-closed）；v1 无主动 Failed 回执，失败发现延迟一个 drain 超时属预期行为

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

### Requirement: shuffle 默认关闭且本地行为不变
网络 shuffle SHALL 默认关闭；关闭时放置、图构建、checkpoint 与 durability 行为 MUST 与现状逐位一致（连通分量整体落位、拆边报错保留）。

#### Scenario: 默认配置行为不变
- **WHEN** 未启用 shuffle 的部署升级到包含本能力的版本
- **THEN** 所有 Job 的放置、执行与 checkpoint 行为与升级前一致，无新端口监听
