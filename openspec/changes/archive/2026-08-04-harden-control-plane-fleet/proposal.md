## Why

ArkFlow 已具备单机控制 API、Hub/Agent 通道和 durable reconciliation，但单机 operation/event 仍是有界内存状态（`crates/arkflow-core/src/runtime.rs:25-35`），Agent 的周期性 report 目前上报空 metrics（`crates/arkflow-server/src/agent.rs:147-175`），Hub API 也主要提供节点、Stream 和配置级命令（`crates/arkflow-server/src/lib.rs:248-295`）。因此系统能够执行控制请求，却还不能为生产 Fleet 提供跨重启追溯、操作者授权、审计和安全的分批配置发布闭环。

现在补齐这些能力，可以在现有 Node → Stream → Config 模型和 reconciliation 基础上形成面向单 Hub、少量 Agent 的可靠运维控制面，而不必提前引入多 Hub 高可用或 DAG 编排的复杂性。

## What Changes

- 持久化 Hub 的 operation、audit event 和 rollout 状态，并支持 Hub 重启后的恢复与查询。
- 引入用户身份、角色和资源级 RBAC；所有控制面写操作记录操作者、目标、请求关联 ID 和结果。
- 将 Fleet 配置发布建模为可恢复的分批 rollout，支持健康门禁、暂停、继续、取消和回滚。
- 扩展 Agent 协议以报告完整运行指标、版本、能力和生命周期状态，并定义兼容性检查。
- 增加 REST 资源与 SSE 状态事件，使 Console 和外部客户端能够消费 rollout、operation、node 和 stream 的实时变化。
- 将 drain、maintenance、resume 和版本/能力状态纳入节点生命周期契约，为后续外部升级系统集成提供基础。

### Non-goals

- 不实现多 Hub 高可用、分片或跨 Hub 调度。
- 不引入 Pipeline/DAG 作为新的核心资源模型。
- 不由 ArkFlow 控制面负责节点二进制下载、替换和完整升级回滚。
- 不在本变更中实现多租户隔离或复杂审批工作流。

## Capabilities

### New Capabilities

- `control-plane-fleet`: Node、Stream、Config、Operation、Rollout 和节点生命周期组成的 Fleet 控制资源与 API。
- `control-plane-identity`: 用户、角色、资源授权和控制面审计主体。
- `control-plane-rollout`: 分批配置发布、健康门禁、暂停、恢复、取消和回滚。
- `control-plane-events`: REST 查询与 SSE 实时事件分发、断线恢复和事件权限。

### Modified Capabilities

- `control-plane-service`: 将现有控制面操作、状态和审计从进程内有界记录扩展为 Hub 持久化的生产语义。
- `control-plane-reconciliation`: 让 rollout 目标、Agent 能力/版本和指标观测参与可恢复收敛判断。
- `control-plane-deployment`: 补充节点 drain、maintenance、版本兼容和安全访问边界。

## Impact

- Rust：扩展 `arkflow-server` 的 SQLite storage、Hub、Agent、HTTP contract/router，以及 `arkflow-core` 的控制面类型和指标快照。
- HTTP：新增身份/审计、rollout、SSE 和能力/版本相关资源；现有 Node、Stream、Config、Operation API 保持兼容并补充持久化语义。
- Console：增加 Fleet rollout、审计、失败诊断和实时状态消费。
- 测试与文档：增加跨重启恢复、RBAC、rollout、SSE、指标上报和协议兼容性测试。
