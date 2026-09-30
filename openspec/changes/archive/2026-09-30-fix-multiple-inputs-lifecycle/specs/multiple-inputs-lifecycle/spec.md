## ADDED Requirements

### Requirement: 重连 SHALL 恰好重启一代读任务

`multiple_inputs` 的 `connect()` 在实例已有运行中的读任务代时，SHALL 先取消并等待该代全部任务退出，再生成新的 token/tracker 并 spawn 新一代——任意时刻每个子 input 至多一个活跃读任务，任务跟踪结构不跨代累积。上一代已入队的数据消息 SHALL 保留（不得因重连丢失），上一代残留的错误消息 SHALL 被清理（不得触发多余的引擎重连）。

#### Scenario: 重连不叠加读任务

- **WHEN** 引擎因 `read()` 返回 `Disconnection` 对同一 `multiple_inputs` 实例再次调用 `connect()`
- **THEN** 上一代读任务被取消并等待退出后才 spawn 新一代，每个子 input 仍只有一个活跃读任务，无重复投递

#### Scenario: 重连清理陈旧错误、保留数据

- **WHEN** 重连发生时内部通道中同时残留上一代的一条 `Err` 消息与若干数据消息
- **THEN** 陈旧 `Err` 被清理，后续 `read()` 不因它再次触发重连；数据消息保持可读且顺序不变

### Requirement: 子 input 错误 SHALL 单次上浮并退出读任务

子 input 的 `read()` 返回任意错误时，对应读任务 SHALL 把该错误单次转发给消费者后退出，不得循环重读——错误的后续处置（重连或失败）由引擎的既有输入错误路径决定。

#### Scenario: 持续出错的子 input 不热循环

- **WHEN** 某子 input 处于每次 `read()` 都立即返回错误的状态
- **THEN** 该错误经 `multiple_inputs::read()` 单次上浮给引擎，读任务退出，不再有紧密循环的错误转发

### Requirement: 内部消息通道 SHALL 有界

`multiple_inputs` 内部转发消息的通道 SHALL 有界（容量与链间边一致，1024）：通道满时子读任务 SHALL 在发送上等待，背压传导至输入源；消费者消亡或组件关闭时，阻塞中的发送 SHALL 被释放且不挂起关停。

#### Scenario: 下游停顿时缓冲有界

- **WHEN** 消费者停止读取而各子 input 持续产出消息
- **THEN** 内部通道达到容量后子读任务在发送处等待，不再累积内存；消费者恢复后发送继续

#### Scenario: 关闭释放阻塞的发送

- **WHEN** 子读任务因通道满在发送处等待且组件被关闭
- **THEN** 等待中的发送被释放，`close()` 在有界时间内完成，不挂死
