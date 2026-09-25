# network-shuffle-data-plane 变更（Delta）

## MODIFIED Requirements

### Requirement: Ack 三态镜像回执

数据 Envelope 跨远程边传输时，其源 ack SHALL 延迟到**所有**下游副本的回执齐备后才完成；下游的持有语义 SHALL 忠实镜像回上游。上游聚合 SHALL 复用既有 fan-out 分支 ack 机制（分支计数、防重复、abort/undo 补偿），不得引入第二套聚合状态机。下游处理失败 abort 该交付时，接收端 SHALL 回发 `Failed(seq)` 回执；上游收到后 SHALL 立即移除对应 pending 条目并 abort 该分支（补偿），而不等待 barrier drain 超时——超时路径降级为 Failed 帧丢失时的兜底。重复 `Failed` 回执 SHALL 幂等（未知 seq 丢弃）。

#### Scenario: 全副本回执后源 ack 完成

- **WHEN** 一条远程边 fan-out 到 2 个下游 subtask，两个副本各自完成本地处理后回发 Acked(seq)
- **THEN** 上游在收到两个 Acked(seq) 后才完成对应 fan-out 分支，源偏移随之推进

#### Scenario: 窗口持有排除出 drain 等待

- **WHEN** 下游因事件时间窗口持有某批次并回发 Held(seq)
- **THEN** 上游对该 ack 调用 mark_held，源链的 barrier drain 等待不包含它；窗口触发后下游回发 Released(seq)，上游调用 release_held 使其回到在途集合

#### Scenario: 处理失败即时中止分支

- **WHEN** 下游 chain 处理某批次失败并 abort 其远程回执
- **THEN** 上游收到 Failed(seq) 后立即移除该 pending 条目并 abort 对应分支，无需等待 barrier drain 超时；本轮 checkpoint 仍按 fail-closed 处理

#### Scenario: Held 控制帧丢失安全降级

- **WHEN** 下游发出的 Held(seq) 因同步投递路径失败而丢失
- **THEN** 上游该 ack 保持在 drain 等待集合，本轮 barrier drain 超时后 checkpoint fail-closed，不产生数据丢失或偏移错推

#### Scenario: 下游崩溃不产生已 ack 丢失

- **WHEN** 下游节点在处理完批次但回执发出前崩溃，或回执在网络中丢失
- **THEN** 上游对应 ack 永不完成，barrier drain 超时使本轮 checkpoint 失败（fail-closed），恢复后从上个 sealed cut 重放（at-least-once）
