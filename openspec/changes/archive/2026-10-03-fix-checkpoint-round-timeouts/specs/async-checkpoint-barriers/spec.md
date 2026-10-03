## ADDED Requirements

### Requirement: Checkpoint 轮次 SHALL 有时间界

checkpoint 轮次的报告收集阶段 SHALL 有整体 deadline（默认 10 分钟）：超时时该轮 SHALL 以显式错误失败（错误点名超时与时长），沿用既有轮次失败语义——保留上一有效 checkpoint、数据面继续处理、下一轮重试。超时放弃后迟到的链报告 SHALL 被吸收（不毒化下一轮）；迟到 barrier 在链内按序处理，SHALL NOT 因协调器放弃该轮而失败链。deadline SHALL 可测试注入。

#### Scenario: 挂起轮次超时失败而非永久停摆

- **WHEN** 一轮 checkpoint 的某个参与链因持续背压或内部挂起而永不报告，超过 deadline
- **THEN** 该轮以显式超时错误失败，上一有效 checkpoint 保留，数据面继续处理，下一轮照常发起

#### Scenario: 超时后的迟到报告被吸收

- **WHEN** 一轮超时放弃后，某链对其 barrier 的报告迟到于下一轮的收集阶段
- **THEN** 下一轮按既有 stale-report 路径忽略该报告（带告警），本轮照常完成，链不失败

#### Scenario: 正常轮次不受影响

- **WHEN** 所有参与链在 deadline 内完成报告
- **THEN** 轮次语义与无 deadline 时逐位一致
