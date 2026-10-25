# add-node-labels

## Why

控制面没有节点标签体系：注册协议无 labels 字段（`crates/arkflow-server/src/hub/wire.rs:8-24`），存储层节点记录无标签列（`crates/arkflow-server/src/storage/mod.rs:36` 仅有 capabilities_json），rollout 目标只能是显式 node_ids 列表（`crates/arkflow-server/src/hub/rollout.rs:14`、`api/rollouts.rs:42`）。这同时阻塞两个方向：多节点运营缺少「按标签批量选择目标」（PLANNING.md 5.6 阶段 3 遗留），配置审批流 v1 只能做全局开关、标签分组策略被显式延后（`configuration-approval` 落地时的已知简化）。规模上去后逐个勾选节点 ID 的运营方式不可持续。

## What Changes

- **节点标签数据模型**：每个节点一份 `labels: map<string,string>`，**Hub 侧管理**（operator 经 API 设置，不随 Agent 重注册漂移）；持久化于 SQLite 与 Postgres（schema 迁移），有界（≤16 个标签，key/value 各 ≤63 字符，限定字符集），变更落审计。
- **Hub API**：节点表示携带 labels；新增 `PUT /nodes/{id}/labels` 全量替换（要求 `ManageNodes` 动作）；`GET /nodes` 支持 label 等值过滤参数。
- **Rollout 标签目标选择**：`CreateRolloutRequest` 新增可选 `target_selector`（等值匹配 map），与显式 `node_ids` 互斥（二选一）；Hub 在**创建时一次性解析**为显式节点集快照——后续标签变更不影响已创建的 rollout；解析结果为空集则拒绝；审批请求钉住解析后的节点集（selector 不进入审批快照语义）。
- **Console**：节点标签展示/编辑（Overview 节点卡片）、rollout 创建提供「按标签选择」模式、节点列表 label 过滤。
- **文档**：控制面 API 文档与运营文档补标签章节（en/zh）。

无破坏性变更：所有新增字段 serde default，旧 Agent/旧客户端行为逐位不变；不设任何标签时 rollout/node API 行为与现状一致。

## Capabilities

### New Capabilities

- `node-labels`: 节点标签的数据模型、管理 API、过滤语义、持久化与审计约束，以及 rollout 目标的标签选择器解析规则。

### Modified Capabilities

- `control-plane-rollout`: 「Create an immutable configuration rollout」需求扩展——目标选择可以是显式 node_ids 或 label selector（创建时解析为显式节点集，空集拒绝，二者互斥），其余 rollout 语义（不可变版本、有界批次、健康门控、暂停/回滚、审批闸门）不变。

## Impact

- **crates/arkflow-server**：`storage/mod.rs`（NodeRecord 增列 + 迁移）、`storage/sqlite.rs`/`postgres.rs`（建表与 `user_version`/schema 迁移）、`hub/nodes.rs`（标签校验与变更）、`api/nodes.rs`（PUT labels 路由）、`api/mod.rs`（路由注册与列表过滤）、`hub/rollout.rs` + `api/rollouts.rs`（selector 解析）、审计接线；测试覆盖迁移、selector 解析、审批交互、RBAC 拒绝。
- **console/**：`api.ts`、`overview.tsx`（标签编辑）、`rollouts.tsx`（标签选择模式）、i18n zh/en 字典。
- **docs/**：`operate/` 控制面文档（http-api-v1、多节点运营）en/zh 同步。
- **不涉及**：数据面（内核/插件）、Agent 协议（Agent 不感知标签）、README 组件清单（无新组件类型）。

## Non-goals

- **Agent 侧声明标签**（register/heartbeat 携带）：v1 标签为 Hub 管理面资源；部署期自动化可经 API 打标签。若后续有 env 声明需求另立 change。
- **标签分组审批策略**（`approval_required` 按 label 分组）：本 change 只交付数据模型与选择器，策略本身为后续 change（`add-approval-label-scoping`）。
- **按标签批量流命令**（批量 start/stop/restart Stream）。
- **标签放置约束**（resource-aware placement 不变；label affinity 另立项）。
- **集合式选择器表达式**（`notin`/`exists`/多值）：v1 仅等值匹配。
