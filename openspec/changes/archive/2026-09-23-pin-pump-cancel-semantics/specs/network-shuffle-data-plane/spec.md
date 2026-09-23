# network-shuffle-data-plane 变更（Delta）

## ADDED Requirements

### Requirement: pump 取消与 void-write 语义

outbound pump 在任何退出路径（wire 写失败、shutdown 取消、上游通道关闭）SHALL 保证：已注册回执但回执不可能到达的分支 SHALL 被 abort（绝不虚假 ack）；清理前 SHALL 对已编码入缓冲的帧尽力 flush；wire 写失败 SHALL 使 pump 以错误退出。由此远程边维持 at-least-once：失败路径的分支由上游重投递，下游允许重复。

#### Scenario: wire 写失败 abort 已注册分支

- **WHEN** pump 在 register 之后、帧送达对端之前遇到 wire 写失败
- **THEN** 该分支被 abort（非 ack），pump 以 Err 退出

#### Scenario: shutdown 时未决回执被 abort

- **WHEN** pump 因 shutdown 取消退出且仍有已注册未回执的分支
- **THEN** 每个未决分支被 abort，已编码帧先经尽力 flush 送达

#### Scenario: 回执确认正常路径

- **WHEN** 对端回执 Acked 到达且副本计数归零
- **THEN** 分支被 ack 并从 pending 移除
