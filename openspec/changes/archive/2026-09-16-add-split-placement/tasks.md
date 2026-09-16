## 1. 放置模型（core）

- [x] 1.1 `JobSpec` 增加 `placement: PlacementStrategy`（`colocated | split`，serde 默认 colocated）；旁路边共位校验落在 `assignments_for_nodes` 的 split 分支（校验需知晓节点数，spec 级 validate 无法判定）；单测：默认反序列化、旁路边拒绝
- [x] 1.2 `assignments_for_nodes` 的 split 分支：按 plan 物理顺序轮转分配（保留 colocated 分支原样）；单测：双节点轮转、确定性、同算子 subtask 散布

## 2. Hub：端口注册与命令载荷

- [x] 2.1 `RegisterRequest` 增加 `data_port: Option<u16>`，入 `NodeRecord`/`HubNode` 持久化；单测：注册携带与查询
- [x] 2.2 job_start 分发路径：split Job 校验期望节点集数据面能力（端口 + capability），载荷注入 `task_nodes` 全量映射与 `node_data_ports`；单测：split 命令载荷内容、无能力节点拒绝、colocated 载荷不变
- [x] 2.3 Agent `start()` 的 `RemoteEdgeContext.task_nodes` 优先取载荷 `task_nodes` 全量映射（缺失时回退本节点 assignment 并告警）；单测：全量映射生效

## 3. 端到端

- [x] 3.1 双节点 split e2e：split Job 经真实 TCP 数据面跨节点路由与聚合 checkpoint 完整跑通（复用 shuffle 测试设施），断言数据行、双副本 barrier、checkpoint 聚合 manifest
- [x] 3.2 colocated 回归：现有 distributed-job-runtime / two_node_job_smoke 测试全绿

## 4. 收尾

- [x] 4.1 `cargo test --workspace --all-targets` 全绿；clippy 新增代码零警告
- [x] 4.2 对照 spec 逐场景核验测试覆盖；`openspec validate` 通过
