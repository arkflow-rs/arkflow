# remote-edge 变更（Delta）

## ADDED Requirements

### Requirement: 写泵退出时无条件 abort 所有 pending branch

写泵（`pump_edge`）在退出时 SHALL 无条件调用 `pending.abort_all()`，不论退出原因是正常关闭、`shutdown.cancelled()` 还是 channel closed。这确保不会有 branch 的源确认被永久泄漏。

#### Scenario: 正常关闭时 abort pending

- **WHEN** 写泵因 `shutdown.cancelled()` 而退出
- **THEN** 所有 pending branch 的源确认被 abort

#### Scenario: channel 关闭时 abort pending

- **WHEN** 边缘发送端 channel 关闭导致写泵退出
- **THEN** 所有 pending branch 的源确认被 abort
