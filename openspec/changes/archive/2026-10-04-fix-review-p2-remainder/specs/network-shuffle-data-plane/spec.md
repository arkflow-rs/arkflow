# network-shuffle-data-plane Delta

## ADDED Requirements

### Requirement: 回执等待读空闲分级
远端连接的 receipt 读循环 SHALL 区分两种读空闲预算：存在待回执的在途批次（pending 非空）时使用 `receipt_wait_timeout`（默认 10 分钟，可配置）；pending 为空时使用 `read_idle_timeout`（默认 30 秒）。下游处理慢导致回执延迟 MUST NOT 在 30 秒预算下被误判为死连接而拆链；pending 为空时的快速死连接发现语义保持不变。

#### Scenario: 慢下游不再触发 30 秒拆链
- **WHEN** 远端连接存在待回执批次且对端因处理背压超过 30 秒未发送任何回执
- **THEN** 连接在 `receipt_wait_timeout` 预算内保持建立，不因读空闲被拆除

#### Scenario: 无在途批次时快速发现死连接
- **WHEN** 远端连接 pending 为空且超过 `read_idle_timeout` 无任何入站帧
- **THEN** 读空闲按既有语义判定失败并拆除连接

## MODIFIED Requirements

### Requirement: 远程边背压有界
跨节点边 SHALL 端到端保持有界缓冲：下游消费停滞时，缓冲压力 SHALL 沿 TCP 反压至上游 chain 的发送点，系统 MUST NOT 无界缓存 Envelope、accepted connections、receipt controls 或 pending acknowledgements。所有由远端声明的 frame 长度 SHALL 在分配前通过配置上限校验。pending 重放缓存 SHALL 同时受条数上限（既有）与近似字节预算（`max_pending_bytes`，默认 256 MiB，按 MessageBatch 内存估算记账）约束：超任一上限时新批次 MUST NOT 注册进 pending，而是走既有发送失败路径；回执信号通道溢出时的升级路径 MUST NOT 同步阻塞 async worker 线程，MUST 以非阻塞尝试 + 错误日志表达（信号最终由读循环停摆引发的连接拆除兜底）。

#### Scenario: 慢消费者反压传播

- **WHEN** 下游 subtask 停止消费且本地入站通道写满
- **THEN** 入站 pump 停止读取 socket，TCP 窗口收缩，上游 chain 的发送调用阻塞而非内存增长

#### Scenario: accepted connection queue reaches its bound

- **WHEN** 新连接到达且已有连接数或 accepted queue 达到配置上限
- **THEN** 新连接被关闭并记录 bounded-resource failure，现有连接和已接收 Envelope 不受影响

#### Scenario: pending 重放超过字节预算

- **WHEN** 已注册 pending 批次的近似字节总和达到 `max_pending_bytes` 且新的发送完成等待注册
- **THEN** 该批次被拒绝注册并走既有发送失败路径（上游分支收到失败信号），重放缓存字节占用保持有界

#### Scenario: 回执信号通道满时不阻塞 worker

- **WHEN** 回执溢出升级路径发现 failures 通道已满
- **THEN** 升级以非阻塞尝试 + 错误日志完成，不发生对有界通道的同步阻塞 send，调用线程（可能位于 tokio worker）立即返回
