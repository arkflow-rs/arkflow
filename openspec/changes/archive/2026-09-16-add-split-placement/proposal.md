## Why

跨节点数据面已落地（`add-network-shuffle-data-plane`，已归档）：远程边以不可区分于本地通道的语义传输 `Envelope`，graph 构建的"拆边即报错"仅在未接线 `RemoteEdgeContext` 时触发，Agent 侧数据面 listener、capabilities 与命令载荷解析均已就绪。但放置算法仍按连通分量整体落位（`crates/arkflow-core/src/job.rs:706-712`，注释明言 "no cross-node shuffle/transport"）——数据面没有消费方，单 Job 无法横向扩展到多节点。本 change 放开放置：Job 显式启用后按 key-group 把算子 subtask 拆到多节点。

## What Changes

- `JobSpec` 新增放置配置（`placement: colocated | split`，默认 `colocated` 保持现行为）；`split` 时 `assignments_for_nodes` 从连通分量整体落位改为**按 task 粒度轮转分配**（sources→operators→sinks 顺序，subtask 均匀散布），并以 `operator_routing_index` 冻结 quad 兼容性。
- Hub 的 job_start 分发为每个节点下发**全量 task→node 映射**与**节点数据端口表**（`node_data_ports`，来自节点注册信息），Agent 据此构建 `RemoteEdgeContext`（数据面已支持该载荷）。
- 节点注册/心跳上报的数据端口（`add-network-shuffle-data-plane` 已在 capabilities 声明）进入 Hub 的节点注册表存储，供放置时构造端口表；未上报数据端口的节点不参与 `split` 放置（放置校验拒绝）。
- 旁路边（error/late-event route）保持共位约束：`split` 放置在校验阶段要求旁路边两端同节点，违反则拒绝放置（沿用 graph.rs 既有硬报错语义）。

**Non-goals**（本 change 明确不做）：

- 不做动态 rescale / 运行时迁移（放置变更仍走 generation 重放置）。
- 不做放置策略优化（负载感知、亲和性）——v1 为确定性轮转。
- 不做跨 Job 的失败通道定界（沿用 shuffle change 的既有限制）。

## Capabilities

### New Capabilities
- `split-placement`：多节点放置策略——split 配置语义、task 粒度分配规则、期望节点集校验（数据端口能力）、旁路边共位约束、Hub 全量映射与端口表下发。

### Modified Capabilities
- `distributed-job-runtime`：放置需求从"连通分量整体落位"扩展为"可按 task 拆分"；完成判定的 expected assignment 集合语义不变（仍是全节点成功）。需 delta spec。

## Impact

- **代码**：`crates/arkflow-core/src/job.rs`（放置配置 + `assignments_for_nodes` 分支 + 校验）、`crates/arkflow-server/src/hub.rs`（节点注册表存数据端口、job_start 命令载荷加映射与端口表、放置校验）、`crates/arkflow-core/src/config.rs`（无新配置，沿用 `data_port`）。
- **协议**：job_start 命令载荷新增 `task_nodes` 与 `node_data_ports`（后者 Agent 侧已解析）；节点注册/心跳已带 capabilities，新增数据端口字段。
- **测试**：放置分配确定性、split 校验（无数据端口节点拒绝、旁路边跨节点拒绝）、命令载荷内容、双节点 e2e（split Job 真实跑通数据面，复用 shuffle 的 loopback/TCP 设施）。
