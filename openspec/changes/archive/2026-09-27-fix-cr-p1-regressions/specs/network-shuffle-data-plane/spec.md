## MODIFIED Requirements

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
