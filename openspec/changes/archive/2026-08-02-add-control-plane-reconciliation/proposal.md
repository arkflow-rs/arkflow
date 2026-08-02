## Why

ArkFlow 已有 Hub/Agent 的命令派发能力，但控制面仍把 `desired_state` 从实际运行状态推导出来（`crates/arkflow-core/src/runtime.rs:290-305`），因此无法表达“节点暂时离线但 Stream 仍应运行”的持久化意图。Hub 当前只保存一次性内存操作记录和命令队列（`crates/arkflow-server/src/hub.rs:153-175`、`350-429`），节点重连或 Hub 重启后不能可靠地恢复和协调未完成目标。

现在需要把控制面从命令代理升级为持续收敛系统：Hub 保存期望状态，Compute Node 上报观测状态，Reconciliation 根据 generation、操作结果和失败策略持续推动两者一致。

## What Changes

- 将 Stream 的期望状态从运行时观测状态中独立出来，并为每次意图变更分配单调递增的 generation。
- 引入 Intent、Attempt、Convergence 三层状态语义，区分用户意图、单次命令尝试和资源最终收敛状态。
- 将 start/stop/configuration 等控制请求建模为可恢复的期望状态变更，而非仅派发一次命令。
- 增加 Reconciliation 生命周期，支持节点重连、Hub 重启恢复、命令幂等、旧 generation 淘汰和有限重试。
- 定义暂时性失败、节点不可用、永久执行失败、结果不确定和被新意图替代的统一语义。
- 将配置版本纳入期望/观测对比，只有 Node 报告目标配置版本和运行状态后才视为收敛。
- 为 restart 等非稳定目标引入一次性 action identity，避免重试造成语义不确定。
- 修改控制面资源和操作 API，使其同时暴露 desired、observed、generation 和 convergence 信息。

## Capabilities

### New Capabilities

- `control-plane-reconciliation`: Hub/Agent 持久化期望状态、协调节点观测状态、处理 generation、重试、失败和重连收敛。

### Modified Capabilities

- `control-plane-service`: Stream 和配置 API 暴露期望状态、观测状态以及收敛状态。

## Impact

- Rust：`arkflow-core::control`、`runtime`、`control_plane`，以及 `arkflow-server::hub`、Agent 协议和 HTTP handlers。
- API：生命周期、配置、操作和 Stream 资源的响应结构及状态语义发生扩展；客户端需要区分 intent/attempt/convergence。
- 状态存储：新增期望状态、协调意图、命令尝试和配置目标的持久化边界；具体存储实现由 design 决定。
- Console：展示 desired/observed/convergence，并将操作完成定义为观测状态达到目标。
- 运维：节点离线不再自动清除运行意图；恢复后会按照持久化目标重新协调。

## Non-goals

- 不在本变更中实现跨节点 Stream 迁移或自动调度。
- 不实现 Hub 集群、leader election 或多活一致性。
- 不改变 Stream 数据面、WAL 交付语义或处理器 backpressure 机制。
- 不引入复杂的用户自定义重试 DSL、租户配额或 rollout 编排策略。
