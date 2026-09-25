# input-durability 变更（Delta）

## MODIFIED Requirements

### Requirement: 优雅关闭时的 parked 确认 drain

WAL 关闭触发时，仍在等待前序 in-flight 交付 settle 的 parked 确认 SHALL 在有界 drain 窗口（15s）内与截止时间竞速等待：前序交付 settle（通知唤醒）后 SHALL 正常完成其源提交并返回成功；计时器先到期 SHALL 返回 `WAL closed while acknowledgement was pending`（恢复时重放，at-least-once 不变）。等待期间若发现前序序列已记录源失败，SHALL 立即返回「被前序源失败阻塞」错误——该路径不经过 drain 窗口。已可执行的确认在 close 后 SHALL 照常完成其源提交。

#### Scenario: 前序交付在窗口内 settle

- **WHEN** 序列 2 的确认 parked 在序列 1 的 in-flight 源提交之后，此时 WAL close 触发，且序列 1 的源提交在 drain 窗口内成功
- **THEN** 序列 2 的确认正常完成并返回成功，序列 1 与 2 均被提交，流关闭不产生错误

#### Scenario: 窗口耗尽回落错误路径

- **WHEN** drain 窗口耗尽时 parked 确认仍未 settle（前序源提交失败或长期阻塞）
- **THEN** 返回 `WAL closed while acknowledgement was pending`，该确认在恢复时重放（at-least-once 不变）

#### Scenario: 前序源失败立即阻塞报错

- **WHEN** 序列 1 的源提交已失败并记录错误，序列 2 的确认变为 parked
- **THEN** 序列 2 立即返回「被前序源失败阻塞」错误，不等待 drain 窗口
