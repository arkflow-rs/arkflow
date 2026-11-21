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

跨节点边 SHALL 端到端保持有界缓冲：下游消费停滞时，缓冲压力 SHALL 沿 TCP 反压至上游 chain 的发送点，系统 MUST NOT 无界缓存 Envelope、accepted connections、receipt controls 或 pending acknowledgements。所有由远端声明的 frame 长度 SHALL 在分配前通过配置上限校验。pending 重放缓存 SHALL 同时受条数上限（既有）与近似字节预算（`max_pending_bytes`，默认 256 MiB，按 MessageBatch 内存估算记账）约束：超任一上限时新批次 MUST NOT 注册进 pending，而是走既有发送失败路径；回执信号通道溢出时的升级路径 MUST NOT 同步阻塞 async worker 线程，MUST 以非阻塞尝试 + 错误日志表达（信号最终由读循环停摆引发的连接拆除兜底）。入站连接数上限 SHALL 可配置（`max_connections`，默认 256）；accepted 队列或握手许可溢出 SHALL 以有界计数器与节流日志呈现，MUST NOT 仅进入无消费者的通道。

#### Scenario: 慢消费者反压传播

- **WHEN** 下游 subtask 停止消费且本地入站通道写满
- **THEN** 入站 pump 停止读取 socket，TCP 窗口收缩，上游 chain 的发送调用阻塞而非内存增长

#### Scenario: accepted connection queue reaches its bound

- **WHEN** 新连接到达且已有连接数或 accepted queue 达到配置上限
- **THEN** 新连接被关闭并记录 bounded-resource failure，现有连接和已接收 Envelope 不受影响

#### Scenario: 连接溢出被计数且可观测

- **WHEN** 入站连接因达到 `max_connections` 上限被拒绝
- **THEN** 拒绝计数递增并产生节流告警日志，计数器有界（不随拒绝次数无界增长）

#### Scenario: pending 重放超过字节预算

- **WHEN** 已注册 pending 批次的近似字节总和达到 `max_pending_bytes` 且新的发送完成等待注册
- **THEN** 该批次被拒绝注册并走既有发送失败路径（上游分支收到失败信号），重放缓存字节占用保持有界

#### Scenario: 回执信号通道满时不阻塞 worker

- **WHEN** 回执溢出升级路径发现 failures 通道已满
- **THEN** 升级以非阻塞尝试 + 错误日志完成，不发生对有界通道的同步阻塞 send，调用线程（可能位于 tokio worker）立即返回

### Requirement: Ack 三态镜像回执

数据 Envelope 跨远程边传输时，其源 ack SHALL 延迟至**所有**下游副本的回执齐备后才完成；下游的持有语义 SHALL 忠实镜像回上游。上游聚合 SHALL 复用既有 fan-out 分支 ack 机制（分支计数、防重复、abort/undo 补偿），不得引入第二套聚合状态机。下游处理失败 abort 该交付时，接收端 SHALL 回发 `Failed(seq)` 回执；上游收到后 SHALL 立即移除对应 pending 条目并 abort 该分支（补偿），而不等待 barrier drain 超时——超时路径降级为 Failed 帧丢失时的兜底。重复 `Failed` 回执 SHALL 幂等（未知 seq 丢弃）。pending receipt 预算（条数或字节）超限时 SHALL 走单次交付的发送失败路径（abort 该分支、上游重投递），MUST NOT 升级为 attempt 或 Job 失败——**但批次自身的 Arrow 内存单独超过字节预算时除外**：该批次永远无法进入重放缓存，abort-and-redeliver 对会回卷位点的源构成无进展活锁，SHALL 以显式错误终止该边（fail-closed）。

#### Scenario: 全副本回执后源 ack 完成

- **WHEN** 一条远程边 fan-out 到 2 个下游 subtask，两个副本各自完成本地处理后回发 Acked(seq)
- **THEN** 上游在收到两个 Acked(seq) 后才完成对应 fan-out 分支，源偏移随之推进

#### Scenario: 窗口持有排除出 drain 等待

- **WHEN** 下游因事件时间窗口持有某批次并回发 Held(seq)
- **THEN** 上游对该 ack 调用 mark_held，源链的 barrier drain 等待不包含它；窗口触发后下游回发 Released(seq)，上游调用 release_held 使其回到在途集合

#### Scenario: 处理失败即时中止分支

- **WHEN** 下游 chain 处理某批次失败并 abort 其远程回执
- **THEN** 上游收到 Failed(seq) 后立即移除该 pending 条目并 abort 对应分支，无需等待 barrier drain 超时；本轮 checkpoint 仍按 fail-closed 处理

#### Scenario: pending 预算超限不升级为 Job 失败

- **WHEN** 单次交付因 pending receipt 条数或字节预算超限而无法注册，且该批次的内存小于字节预算
- **THEN** 仅该交付走发送失败路径（分支 abort、上游重投递），attempt 与 Job 不因此失败

#### Scenario: 永久超过字节预算的批次终止该边

- **WHEN** 一个批次的 Arrow 内存单独超过 `max_pending_bytes`（即使重放缓存为空也无法注册）
- **THEN** 该边以显式错误 fail-closed，而非反复 abort-重投递形成无进展循环

#### Scenario: Held 控制帧丢失安全降级

- **WHEN** 下游发出的 Held(seq) 因同步投递路径失败而丢失
- **THEN** 上游该 ack 保持在 drain 等待集合，本轮 barrier drain 超时后 checkpoint fail-closed，不产生数据丢失或偏移错推

#### Scenario: 下游崩溃不产生已 ack 丢失

- **WHEN** 下游节点在处理完批次但回执发出前崩溃，或回执在网络中丢失
- **THEN** 上游对应 ack 永不完成，barrier drain 超时使本轮 checkpoint 失败（fail-closed），恢复后从上个 sealed cut 重放（at-least-once）

### Requirement: 远程边失败 fail-closed

远程边连接断开、协议校验失败、认证失败、资源上限溢出或对端不可达时，内核 SHALL 使受影响的 Job attempt 失败并清理该边两端资源，MUST NOT 让链路停留在半开状态继续产出或丢弃数据——**但在重连预算内除外**：配置了 `reconnect_attempts > 0` 时，流级失败（任一半程的传输错误）SHALL 在预算次数内以退避重拨并重放全部未回执数据帧；预算耗尽、会话已移除或全局关停 SHALL 走 fail-closed 路径。确定性协议/认证错误（非法帧、认证/完整性校验失败、nonce 重放、quad/generation 不匹配）SHALL 立即 fail-closed 并终止 attempt，MUST NOT 消耗重连预算或被后续连接的成功掩盖；重连预算仅用于传输级瞬时故障（EOF、空闲超时、重置）。`reconnect_attempts = 0` SHALL 逐位保持立即 fail-closed 行为。重连预算与宽限 SHALL 可由运维配置（Agent 启动配置/环境变量），使断边恢复窗口这一隐式 SLO 显式可调；未配置时维持既有默认值。

#### Scenario: 断链失败传播

- **WHEN** 一条已建立的远程边 TCP 连接中断且重连预算为零
- **THEN** 双方对应链路以错误终止（同一 TCP 连接双向同时失败），attempt 进入失败上报，由既有 generation fencing 重新放置

#### Scenario: 预算内透明恢复

- **WHEN** 一条已建立的远程边因网络瞬断中断且重连预算未耗尽
- **THEN** 上游重拨并对端去重后恢复投递，未回执帧重放，双方不产生失败上报，作业不中断

#### Scenario: 运维调大重连预算延长恢复窗口

- **WHEN** 运维将 `reconnect_attempts` 与 `reconnect_grace` 配置为更大的值后发生一次 60 秒网络分区
- **THEN** 受影响边在预算与宽限内透明恢复，不产生失败上报；配置未变时 30 秒分区按既有默认行为失败

#### Scenario: 认证失败不进入重连

- **WHEN** 一条可重连边收到帧认证失败（确定性协议错误）
- **THEN** 该 attempt 立即 fail-closed 上报，重连预算不被消耗，也不会因后续连接碰巧成功而掩盖该错误

#### Scenario: 预算耗尽 fail-closed

- **WHEN** 重连尝试全部失败
- **THEN** 分支经中止路径结算，失败上报，attempt 按围栏重放置

#### Scenario: 非法或未认证帧到达

- **WHEN** 已连接的 peer 发送非法帧、未认证帧或不属于当前 quad/generation 的帧
- **THEN** 连接被关闭、pending ack 被终止、attempt 进入 fail-closed 路径且不交付该帧

#### Scenario: kernel 取消清理连接

- **WHEN** Job kernel 因重新放置或停止被取消
- **THEN** 其建立的远程边出站连接与入站 listener 会话全部关闭，无残留 socket、registry 与悬持 ack

### Requirement: 线协议帧编解码

跨节点传输 SHALL 使用定长帧头（src/dst 四元组、长度、类型）+ Arrow IPC 数据负载 + serde 控制负载 + 帧尾认证 tag 的 v2 线格式；数据帧 MUST 能在接收端还原为等值 RecordBatch（含字典编码列）。接收端 SHALL 在任何 Flatbuffer 或 Arrow body 切片前验证 piece、消息和 body 的边界。数据帧 IPC body 的 Arrow Buffer 构建 MUST NOT 对 body 字节做整体二次拷贝（SHALL 以共享所有权切片承载）。单帧长度上限 SHALL 可配置（`max_frame_len`，默认 256 MiB），发送与接收两侧使用同一配置值。

#### Scenario: 数据帧往返等值

- **WHEN** 一个含字典编码字符串列的 RecordBatch 经帧编码、TCP 传输、帧解码
- **THEN** 接收端还原出与发送端等值的 RecordBatch

#### Scenario: 非法帧头拒绝

- **WHEN** 接收端读到未知帧类型或长度超出上限的帧头
- **THEN** 连接以协议错误关闭，不尝试继续解析后续字节

#### Scenario: 声明的 Arrow body 超出 piece

- **WHEN** IPC 消息的 bodyLength 大于 piece 中实际剩余字节数
- **THEN** 解码返回错误并关闭连接，而不是触发越界 panic

#### Scenario: IPC body 零拷贝

- **WHEN** 解码一个携带非空 IPC body 的数据帧
- **THEN** 构建的 Arrow Buffer 与接收负载共享同一内存（无 body 全量二次拷贝）

#### Scenario: 单帧上限可配置

- **WHEN** 部署将 `max_frame_len` 配置为低于默认值且上游发送超限帧
- **THEN** 发送侧拒绝编码并走发送失败路径，接收侧超限帧头触发协议错误

### Requirement: shuffle 默认关闭且本地行为不变
网络 shuffle SHALL 默认关闭；关闭时放置、图构建、checkpoint 与 durability 行为 MUST 与现状逐位一致（连通分量整体落位、拆边报错保留）。

#### Scenario: 默认配置行为不变
- **WHEN** 未启用 shuffle 的部署升级到包含本能力的版本
- **THEN** 所有 Job 的放置、执行与 checkpoint 行为与升级前一致，无新端口监听

### Requirement: pump 取消与 void-write 语义

outbound pump 的退出路径 SHALL 区分两类清理责任：

- **wire 写失败与 shutdown 取消**：连接正在拆除或进程正在关闭，回执不可能再到达。pump SHALL 在退出前对已编码入缓冲的帧尽力 flush，随后将所有已注册未回执的分支 abort（绝不虚假 ack），并以错误（wire 失败）或干净（shutdown）状态退出。
- **上游通道关闭（干净排空）**：仅发送半边结束，同一连接的回执读循环仍在运行。pump SHALL NOT abort 已注册分支——对端仍可能为已送达帧回执；迟到回执 SHALL 经回执读循环正常应用（ack 或 abort 镜像语义）。最终清理 SHALL 由回执读循环的退出路径拥有：其对端关闭后仍有未回执分支、或读空闲超时时 SHALL 产生失败并 abort 全部未决分支，干净读完（无未决分支）时 SHALL 静默 abort（此时为空操作）。

由此远程边维持 at-least-once：失败路径的分支由上游重投递，下游允许重复；干净排空路径不因过早 abort 而把已送达交付降级为重放。

#### Scenario: wire 写失败 abort 已注册分支

- **WHEN** pump 在 register 之后、帧送达对端之前遇到 wire 写失败
- **THEN** 该分支被 abort（非 ack），pump 以 Err 退出

#### Scenario: shutdown 时未决回执被 abort

- **WHEN** pump 因 shutdown 取消退出且仍有已注册未回执的分支
- **THEN** 每个未决分支被 abort，已编码帧先经尽力 flush 送达

#### Scenario: 通道干净排空后迟到回执仍被应用

- **WHEN** 上游通道关闭使 pump 以 Ok 退出，某已注册分支的 Acked 回执随后经回执读循环到达
- **THEN** 该分支被 ack（非 abort），不触发上游重放

#### Scenario: 排空后对端无回执由读循环兜底失败

- **WHEN** pump 干净退出后对端不再发送任何回执且读空闲超时触发，此时仍有未回执的已注册分支
- **THEN** 回执读循环产生连接失败并 abort 全部未决分支，上游按失败路径重投递

#### Scenario: 回执确认正常路径

- **WHEN** 对端回执 Acked 到达且副本计数归零
- **THEN** 分支被 ack 并从 pending 移除

### Requirement: 重放帧按投递级去重

接收端 SHALL 按会话键记忆已投递到本地通道的最大数据帧 seq（仅在本地通道成功接收后推进）；重放帧 seq ≤ 该值时 SHALL 静默丢弃——不向本地通道重投递，也**不得**补发任何回执：上游分支只由原始投递经处理完成后的真实回执结算。seq 大于该值的帧正常投递并推进记录。会话随 Job 移除或确认丢失时记录与回执路由同步清除。

去重记录 SHALL 单调推进：任何路径不得将其回写为更小的值；"查重 → 本地通道投递 → 推进记录"对同一会话键 SHALL 串行化——重连重叠期（旧连接的投递仍在本地通道排队、新连接已开始服务同一会话键）不得把同一 seq 投递到本地通道两次，也不得因迟到的旧投递完成而回退水位。

#### Scenario: 重放的去重帧被静默丢弃

- **WHEN** 透明重连重放了一个接收端已投递的帧
- **THEN** 本地通道不收到重复交付，且不产生镜像回执——上游等待原始投递的处理完成回执

#### Scenario: 首投帧正常通过

- **WHEN** 一个 seq 大于已投递记录的新帧到达
- **THEN** 帧投递到本地通道，接收成功后推进该会话的去重记录

#### Scenario: 重连重叠不双投递也不回退水位

- **WHEN** 透明重连期间同一会话键被两条连接先后服务：旧连接对某 seq 的投递仍在本地通道反压中排队（尚未完成），新连接重放了同一 seq
- **THEN** 该 seq 至多投递一次到本地通道；旧连接迟到的投递完成不得把去重水位改小，后续更高 seq 的重放仍被正确识别为重复

### Requirement: 接收端丢连在宽限期内挂起失败

配置重连时，连接以 EOF-无-Eos 结束 SHALL 把失败报告延迟 `reconnect_grace`；宽限内同会话键被新连接重新注册即抑制报告并保留路由注册，超时未恢复才报告失败并清除注册。协议级错误不延迟。宽限观察以注册代数为準，不依赖时钟比较。`reconnect_grace` 与 `registration_grace` SHALL 可由运维配置（Agent 启动配置/环境变量），未配置时维持既有默认值（10 秒）。

#### Scenario: 宽限内重注册抑制失败

- **WHEN** 连接丢失后对端在宽限期内重拨并重新注册同一会话键
- **THEN** 不产生失败上报，新连接即刻复用既有路由

#### Scenario: 宽限超时确认丢失

- **WHEN** 宽限期满仍无同键重注册
- **THEN** 失败上报发出，路由注册与认证期望被清除

#### Scenario: 宽限窗口随配置伸缩

- **WHEN** 运维将 `reconnect_grace` 配置为 45 秒后发生一次 30 秒分区
- **THEN** 断边在宽限内被对端重拨恢复，不产生失败上报（默认 10 秒配置下该分区会失败）

### Requirement: 回执经会话级路由跨越连接更替

回执 SHALL 发往按会话键存在的有界会话队列，由常驻转发任务写往当前服务该会话的连接；连接更替时写失败的原条目 SHALL 原样重试直至新连接接槽，不得丢弃。RemoteAck SHALL 持会话队列而非连接通道。连接退出清槽 SHALL 以通道身份为守卫——只清除本连接安装的槽，不得清掉重连替代连接已安装的槽。转发任务 SHALL 同时监视会话撤销与节点数据面关停，二者任一发生即退出。确认丢失与 Job 会话移除 SHALL 撤销路由并取消转发任务。

#### Scenario: 替代连接的槽不被旧连接清理清除

- **WHEN** 透明重连的新连接已安装回执槽，随后旧连接的退出清理运行
- **THEN** 新连接的槽保持有效，其回执照常转发

#### Scenario: 数据面关停停止转发任务

- **WHEN** 节点数据面关停（manager shutdown）
- **THEN** 所有会话转发任务退出，不驻留、不强持 manager

### Requirement: 回执等待读空闲分级
远端连接的 receipt 读循环 SHALL 区分两种读空闲预算：存在待回执的在途批次（pending 非空）时使用 `receipt_wait_timeout`（默认 10 分钟，可配置）；pending 为空时使用 `read_idle_timeout`（默认 30 秒）。下游处理慢导致回执延迟 MUST NOT 在 30 秒预算下被误判为死连接而拆链；pending 为空时的快速死连接发现语义保持不变。

#### Scenario: 慢下游不再触发 30 秒拆链
- **WHEN** 远端连接存在待回执批次且对端因处理背压超过 30 秒未发送任何回执
- **THEN** 连接在 `receipt_wait_timeout` 预算内保持建立，不因读空闲被拆除

#### Scenario: 无在途批次时快速发现死连接
- **WHEN** 远端连接 pending 为空且超过 `read_idle_timeout` 无任何入站帧
- **THEN** 读空闲按既有语义判定失败并拆除连接

### Requirement: 跨节点帧 SHALL 逐帧认证且密钥按会话派生

数据面握手 SHALL 交换双向 nonce：客户端握手携带客户端 nonce，服务端确认 SHALL 携带自身的新鲜 nonce。双方 SHALL 以 HKDF-SHA256 从共享秘密与双向 nonce 派生每连接、每方向（c2s/s2c）独立的会话密钥。握手之后的每一帧（Data/Signal/Receipt）SHALL 携带截断 HMAC 认证 tag，MAC 输入 SHALL 覆盖帧头、帧负载与连接级单调发送序号；接收端 SHALL 校验 tag 且要求序号严格递增，任何验证失败 SHALL 视为确定性协议错误并断连（不进入重连预算）。线协议版本 SHALL 升至 v2 且不与 v1 互通；版本不匹配维持既有协议错误 fail-closed 路径。机密性（payload 加密）仍由 opt-in mTLS 承担，本需求不引入默认加密。

#### Scenario: 无密钥的伪造帧被拒绝

- **WHEN** 未持共享秘密的攻击者向数据面连接注入任意 Data/Signal 帧（或伪造握手后的帧）
- **THEN** 接收端 tag 校验失败，连接以协议错误关闭，帧不进入任何本地通道或 checkpoint 路径

#### Scenario: 篡改帧头或负载被拒绝

- **WHEN** 同网段攻击者截获并修改一个合法帧的帧头（如改写 quad 或长度）或负载字节后转发
- **THEN** 接收端 tag 校验失败并断连，被篡改帧不被交付

#### Scenario: 跨会话重放被拒绝

- **WHEN** 攻击者把先前连接捕获的合法帧在新连接上重放（握手 nonce 已更换，会话密钥不同）
- **THEN** 新会话密钥下 tag 校验失败；同一连接内重放的旧序号帧被序号校验拒绝

#### Scenario: 合法帧流不受影响

- **WHEN** 持正确秘密的两端按 v2 协议握手并互发 Data/Signal/Receipt 帧
- **THEN** 全部帧通过 tag 与序号校验，交付语义与 v1 完全一致（FIFO、去重、回执路由不变）

### Requirement: 数据面秘密 SHALL 独立配置且不回退共享凭证

数据面共享秘密 SHALL 仅来自显式配置（`ARKFLOW_DATA_PLANE_SECRET`）；MUST NOT 回退到节点注册凭证（`node_token`）或其他舰队级共享凭证。未配置秘密时节点 SHALL 走既有 colocated-only 降级路径（不启动数据面 listener、放置仅共置），并以告警日志声明降级原因。秘密 MUST NOT 序列化进 Job 命令、日志或审计。

#### Scenario: 未配置秘密时退化 colocated-only

- **WHEN** Agent 启动时 `ARKFLOW_DATA_PLANE_SECRET` 未设置或为空
- **THEN** 数据面不启动（无 listener、无凭证），节点照常注册并参与共置放置，日志记录降级原因

#### Scenario: 配置独立秘密后不共享注册凭证

- **WHEN** Agent 配置了 `ARKFLOW_DATA_PLANE_SECRET`
- **THEN** 数据面会话密钥完全派生自该独立秘密，节点注册凭证不参与任何数据面计算

