## ADDED Requirements

### Requirement: 多输入 watermark 聚合 SHALL 排除空闲输入

链内 watermark 聚合 SHALL 记录每个输入最近一次 watermark 到达时刻：活跃输入中超过空闲阈值（默认 5 分钟）未上报者，SHALL 被排除出"全员已上报"门与最小值计算（与已结束输入同待遇），使聚合 watermark 不因单个静默源而冻结。被排除输入后续再上报 watermark 时 SHALL 重新加入计算；转发给下游的 watermark SHALL 与上次转发值取最大以保持单调。空闲输入重新加入后因其水位较低而被钳制的行，SHALL 按既有 late 策略处理。

#### Scenario: 静默源不再冻结聚合

- **WHEN** 多输入链的一个输入在超过空闲阈值的时间里未产生任何 watermark，其余输入持续上报
- **THEN** 聚合 watermark 基于其余输入推进，下游窗口触发与状态清理不再冻结

#### Scenario: 空闲源恢复上报

- **WHEN** 被排除的输入恢复上报且其 watermark 低于当前已转发值
- **THEN** 转发值保持单调不回退；该输入重新参与后续聚合，其间迟到行按 late 策略处置

#### Scenario: 未超阈值前语义不变

- **WHEN** 所有活跃输入都在阈值内上报过 watermark
- **THEN** 聚合行为与既有语义逐位一致（全员门 + 最小值）

### Requirement: event-time gate 的持有量 SHALL 有界

event-time gate 因等待 watermark 而持有的行 SHALL 有累计行数上限（默认 1,048,576）：超限时 SHALL 从最旧的持有批开始驱逐并以其 ack 的 abort 结算，节流日志 SHALL 记录累计驱逐规模与后果语义。abort 结算 SHALL 波及该投递的整个 fan-out 组——组内仍被下游持有的兄弟确认在停顿解除（或其在组内排队结算）时 SHALL 失败并按 at-least-once 重启任务（驱逐是防无界增长的最后兜底，接受该 ricochet）；驱逐后重启重放的行 SHALL 落后于已恢复水位并按既有 late 策略处置（Drop 策略即丢弃）。上限未触达时持有语义不变（watermark 推进即释放）。

#### Scenario: 超限驱逐最旧持有

- **WHEN** `held` 累计行数超过上限且 watermark 仍未推进
- **THEN** 最旧的持有批被移除且其 ack 被 abort（从不确认），内存占用回落到上限内，节流 warn 记录累计驱逐行数与 abort 的波及语义

#### Scenario: 驱逐毒化同组兄弟确认

- **WHEN** 被驱逐批与仍在下游持有的兄弟切片同属一个 fan-out 投递，停顿解除后兄弟尝试确认
- **THEN** 兄弟确认失败（fan-out 已 abort）、父确认不发生，投递整体回滚——该失败机制有测试钉住；任务级重启后果由引擎既有失败路径承担（at-least-once）

#### Scenario: 未超限时无驱逐

- **WHEN** 持有行数在上限内且 watermark 推进
- **THEN** 既有释放路径逐位不变，不发生驱逐
