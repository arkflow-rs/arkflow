## ADDED Requirements

### Requirement: Per-chain runtime hooks SHALL be decomposed by concern without semantic change

内核按链分发的运行时钩子 SHALL 按关注点拆解为内聚结构：纯 checkpoint 钩子（快照上报、失败上报、barrier 接收、状态后端、任务身份、链完成上报）、事件时间绑定（watermark 门控、分区绑定）与运行时指标三者分离，共同组成每链钩子集合。拆解 SHALL 保持全部既有语义：barrier 快照上报与失败路径、watermark lag 与迟到事件记录、input/output/error 计数、链退出上报的行为与既有内核测试断言原样通过。

#### Scenario: 拆解后 checkpoint 语义不变

- **WHEN** barrier 经过任一链（源链/状态链/内部链）
- **THEN** 快照上报、失败上报（不终止数据面）与链退出豁免行为与拆解前一致，既有 barrier/checkpoint 测试原样通过

#### Scenario: 拆解后事件时间与指标语义不变

- **WHEN** 源链带事件时间门控与运行时指标运行
- **THEN** watermark lag 记录、迟到事件计数与 input/output/error 计数与拆解前一致，既有 event-time 与 metrics 测试原样通过
