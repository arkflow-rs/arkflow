# network-shuffle-data-plane 变更（Delta）

## MODIFIED Requirements

### Requirement: 远程边失败 fail-closed

远程边连接断开、协议校验失败、认证失败、资源上限溢出或对端不可达时，内核 SHALL 使受影响的 Job attempt 失败并清理该边两端资源，MUST NOT 让链路停留在半开状态继续产出或丢弃数据——**但在重连预算内除外**：配置了 `reconnect_attempts > 0` 时，流级失败（任一半程的传输错误）SHALL 在预算次数内以退避重拨并重放全部未回执数据帧；预算耗尽、会话已移除或全局关停 SHALL 走 fail-closed 路径。确定性协议错误（非法帧、认证失败）重放必然复现，按预算耗尽处理。`reconnect_attempts = 0` SHALL 逐位保持立即 fail-closed 行为。

#### Scenario: 断链失败传播

- **WHEN** 一条已建立的远程边 TCP 连接中断且重连预算为零
- **THEN** 双方对应链路以错误终止（同一 TCP 连接双向同时失败），attempt 进入失败上报，由既有 generation fencing 重新放置

#### Scenario: 预算内透明恢复

- **WHEN** 一条已建立的远程边因网络瞬断中断且重连预算未耗尽
- **THEN** 上游重拨并对端去重后恢复投递，未回执帧重放，双方不产生失败上报，作业不中断

#### Scenario: 预算耗尽 fail-closed

- **WHEN** 重连尝试全部失败
- **THEN** 分支经中止路径结算，失败上报，attempt 按围栏重放置

#### Scenario: 非法或未认证帧到达

- **WHEN** 已连接的 peer 发送非法帧、未认证帧或不属于当前 quad/generation 的帧
- **THEN** 连接被关闭、pending ack 被终止、attempt 进入 fail-closed 路径且不交付该帧

#### Scenario: kernel 取消清理连接

- **WHEN** Job kernel 因重新放置或停止被取消
- **THEN** 其建立的远程边出站连接与入站 listener 会话全部关闭，无残留 socket、registry 与悬持 ack

## ADDED Requirements

### Requirement: 重放帧按投递级去重

接收端 SHALL 按会话键记忆已投递到本地通道的最大数据帧 seq；重放帧 seq ≤ 该值时 SHALL 丢弃（不投递本地通道）并补发 Acked 回执（上游回执应用幂等，双回执安全）。seq 大于该值的帧正常投递并推进记录。会话随 Job 移除时记录同步清除。

#### Scenario: 重放的去重帧被丢弃且回执

- **WHEN** 透明重连重放了一个接收端已投递的帧
- **THEN** 本地通道不收到重复交付，上游收到该 seq 的 Acked 回执并可完成分支

#### Scenario: 首投帧正常通过

- **WHEN** 一个 seq 大于已投递记录的新帧到达
- **THEN** 帧投递到本地通道并推进该会话的去重记录

### Requirement: 接收端丢连在宽限期内挂起失败

配置重连时，连接以 EOF-无-Eos 结束 SHALL 把失败报告延迟 `reconnect_grace`；宽限内同会话键被新连接重新注册即抑制报告并保留路由注册，超时未恢复才报告失败并清除注册。协议级错误不延迟。宽限观察以注册代数为準，不依赖时钟比较。

#### Scenario: 宽限内重注册抑制失败

- **WHEN** 连接丢失后对端在宽限期内重拨并重新注册同一会话键
- **THEN** 不产生失败上报，新连接即刻复用既有路由

#### Scenario: 宽限超时确认丢失

- **WHEN** 宽限期满仍无同键重注册
- **THEN** 失败上报发出，路由注册与认证期望被清除
