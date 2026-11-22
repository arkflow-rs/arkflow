## ADDED Requirements

### Requirement: Pulsar input 的接收退避 SHALL 由 RetryConfig 经共享实现治理

Pulsar input 的消息接收退避 SHALL 由配置的 `RetryConfig`（`max_attempts`/`initial_delay_ms`/`backoff_multiplier`/`max_delay_ms`）经共享重试实现执行——校验过的策略 MUST 实际作用于接收循环，MUST NOT 出现"配置校验后弃用、另写手写循环"的分裂；退避期间的取消请求 MUST 保持既有取消安全语义（立即中断退避等待）。

#### Scenario: 配置的重试策略实际生效
- **WHEN** Pulsar input 配置 `initial_delay_ms: 1000` 并遭遇暂态接收失败
- **THEN** 重试等待按配置的 1s 起步（而非手写循环的内置值）

#### Scenario: 退避期间取消立即返回
- **WHEN** 接收循环处于退避等待中且组件被取消
- **THEN** 等待立即中断并进入取消路径，不等待退避计时器到期

#### Scenario: 重试耗尽归 Disconnection
- **WHEN** 接收失败重试至 `max_attempts` 耗尽
- **THEN** 返回 `Error::Disconnection` 交内核重连（与 `connector-recovery-contract` 一致）
