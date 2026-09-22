# Capability: Test Stability

## Purpose

Define the determinism contract for long-running integration tests: bounded waits followed by explicit task aborts at teardown (so nothing outlives a test and panics after the summary), and generous ceilings for scheduler-dependent async propagation waits (which only observe conditions that normally complete within a tick). (Purpose derived from the `harden-test-stability` change; refine as the capability evolves.)

## Requirements

### Requirement: 集成测试的确定性 teardown

 spawning 后台任务（Hub/Agent/reconcile 等）的集成测试 SHALL 在结束时以「有界超时等待 + 显式 abort()」收尾：等待 cancellation 生效后，仍未退出的任务 SHALL 被显式 abort，SHALL NOT 留待 runtime drop 时被轮询。需要验证「任务必须停止」的断言 SHALL 使用 ≥15s 的预算以容忍负载下的调度延迟。

#### Scenario: 忽略取消令牌的任务不外泄

- **WHEN** 测试结束时某后台任务未在其取消令牌触发后 5s 内退出
- **THEN** 测试显式 abort 该任务，进程在测试摘要之后不发生任何 teardown panic

#### Scenario: 停止断言容忍负载

- **WHEN** 全工作区测试并行运行导致调度延迟
- **THEN** 「任务必须停止」断言以 15s 预算评估，不因负载误报

### Requirement: 异步传播等待的预算上限

依赖调度器的异步传播观察（断连 abort、远端操作完成等）在测试中 SHALL 使用不低于 30s 的等待上限，SHALL NOT 以正常路径延迟（~100ms 量级）作为预算依据；这些等待仅观察正常情况下必然发生的条件，放宽上限零成本。

#### Scenario: 负载下断连传播不误报

- **WHEN** 全工作区测试并行运行、tokio 定时器延迟数秒
- **THEN** 断连/abort 传播测试仍能观察到条件成立，不因预算击穿而失败
