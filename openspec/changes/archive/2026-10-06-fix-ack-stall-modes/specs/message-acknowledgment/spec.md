## ADDED Requirements

### Requirement: 终态投递的后续 undo SHALL 幂等无害
已 abort 的投递 SHALL 视为终态：其后到达的任何 `undo`（来自缓冲算子补偿链的迟到调用）SHALL 为幂等 no-op——不得把已完成结算回退、不得使 barrier 阻塞计数（dispatched − completed − held）复活。成功 `ack` 后的 `undo` 语义保持既有回退行为；`abort` 自身幂等保持。

#### Scenario: abort 后迟到的 undo 不复活阻塞计数

- **WHEN** 一个投递经 `abort()` 终态结算后，缓冲算子的补偿链再对其调用 `undo()`
- **THEN** tracker 的阻塞计数保持 0（该投递不再阻塞 barrier），undo 无副作用

#### Scenario: 正常 undo 语义不变

- **WHEN** 一个成功 ack 的投递被 undo（无 abort 参与）
- **THEN** 其结算被回退、重新计入下一轮 checkpoint 的等待集合（既有行为）

#### Scenario: abort 幂等

- **WHEN** 同一投递被连续 abort 两次
- **THEN** tracker 计数只结算一次（既有行为保持）
