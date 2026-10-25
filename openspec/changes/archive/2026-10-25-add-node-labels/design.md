# add-node-labels — Design

## Context

控制面节点无标签：`cp_nodes` 表（`storage/sqlite.rs:2376`）的最近似先例是 `capabilities_json`（`storage/mod.rs:36`）——JSON 文本列、Agent 注册时上报。rollout 目标选择是显式 `node_ids: Vec<String>`（`hub/rollout.rs:14`、`hub/approval.rs:28`），节点操作（drain/maintenance）授权用 `OperatorAction::ManageNodes`（`api/nodes.rs:94`），rollout 创建不做 per-node scope 校验。双后端 schema 演进机制已就绪：SQLite `PRAGMA user_version` + PG `cp_schema_meta`（`storage/sqlite.rs:2765`、`postgres.rs:763`），PG 已有 `ADD COLUMN IF NOT EXISTS` 先例（`postgres.rs:714`）。

## Goals / Non-Goals

**Goals:**

- 节点标签数据模型（持久化、有界、可审计）与 Hub 管理 API。
- `GET /nodes` 标签过滤、rollout 创建的标签选择器（创建时一次解析为显式节点集）。
- Console 标签编辑与按标签创建 rollout。
- 旧客户端/旧 Agent 零影响（全部新增字段 serde default / 可选参数）。

**Non-Goals:**

- Agent 侧声明标签、标签分组审批策略、按标签批量流命令、标签放置约束、集合式选择器（`notin`/`exists`）——见 proposal。

## Decisions

### D1. 标签为 Hub 单源管理，Agent 不感知

标签只经 Hub API 由 operator 设置；register/heartbeat/report 协议（`hub/wire.rs`）不加字段。

- 为什么：K8s 同构（node labels 是 API-managed，kubelet 不上报标签）；Agent 重启/重注册不会冲掉 operator 的分组意图；无需定义双源合并优先级。
- 备选否决：① Agent 从 env 声明（`ARKFLOW_NODE_LABELS`）——便利但引入"重注册刷新 vs operator 覆盖"的优先级语义；② 双源合并（agent ∪ operator，operator 胜）——v1 复杂度不成比例。若后续有 env 声明诉求，可在 operator 源之上加 agent 源（向前兼容）。
- 部署期自动化（Helm 等）经 API 打标签即可，无额外机制。

### D2. 存储：`cp_nodes` 增 `labels_json TEXT NOT NULL DEFAULT '{}'` 列

沿用 `capabilities_json` 先例（JSON 文本列而非子表/EAV）：节点标签 ≤16 条，整读整写，无按标签索引查询的运维表规模（≤256 节点）。

- 迁移：两后端均走既有机制——SQLite 以 `PRAGMA table_info` 守卫 + 条件 `ALTER TABLE cp_nodes ADD COLUMN labels_json TEXT NOT NULL DEFAULT '{}'`（`storage/sqlite.rs:2698-2727` 已有 cp_nodes 加 `node_version`/`maintenance_state` 列的完全相同先例；`PRAGMA user_version` 只是迁移跑完后的盖章，不是触发器）；PG `ADD COLUMN IF NOT EXISTS` + `cp_schema_meta` 版本推进（`postgres.rs:714` 先例）。新建库的 `CREATE TABLE cp_nodes` 同步加列，DEFAULT 使无标签 INSERT 不必携带该列。常量默认值在两后端均为合法 DDL。
- 读写：`NodeRecord` 增 `labels: BTreeMap<String, String>`（BTreeMap 保证序列化确定序，便于快照/测试断言），行映射层与 `capabilities_json` 同法。
- **实现红线（CR 确认）**：`upsert_node` 的 INSERT/ON CONFLICT 列清单是显式集合（`sqlite.rs:403`、`postgres_methods.rs:180` 同一 SQL 文本），**不得**把 labels_json 加进 `NodeMutation` 或该 SQL——列不在清单里即天然不被 register/heartbeat/report 的 upsert 触碰，"重注册不冲标签"正是靠这一点免费成立；一旦把空 labels 塞进 upsert，每次 Agent 重注册都会冲掉 operator 标签。标签的唯一写路径是 `replace_node_labels` 专用方法（独立 UPDATE 语句）。
- **注册回灌与并发 PUT 的全序（CR 二轮确认）**：注册的预插入回灌读与 `replace_node_labels` 的"存储写→内存写"存在竞态窗口（窗口内 selector 会解析到过期标签）。修复：注册在 upsert 完成后，于**单次持有节点表写锁期间**完成"重读存储+补内存"——PUT 的内存写需要同一把锁，故任一 PUT 要么存储写在我们的读之前（被读到），要么内存写在我们的释放之后（覆盖我们），内存态不可能以过期标签结束本次注册。晚期重读失败不使注册失败（upsert/游标重置等副作用已发生）；早期回灌读失败则照常拒绝（尚无副作用）。
- 备选否决：独立 `cp_node_labels` 表——为 ≤16×256 规模引入 join 与第二套迁移面不值。

### D3. 变更语义：`PUT /nodes/{id}/labels` 全量替换，授权 `ManageNodes`

提交的 map 即节点完整标签集（幂等、无 patch 冲突语义）。授权与 drain/maintenance 同动作（`OperatorAction::ManageNodes`，`api/nodes.rs:94` 先例），走既有 node-scope 判定；接受与拒绝均落审计（复用 `record_audit` 通路，action 取 `node_labels_replace`）。

- 备选否决：PATCH 增量合并——并发合并语义 + 部分失败回滚复杂度，v1 无消费方。

### D4. 选择器：创建入口一次解析，复用既有 node_ids 路径

`CreateRolloutRequest` 增可选 `target_selector: Option<BTreeMap<String,String>>`。Hub 在**进入现有创建逻辑之前**将其解析为显式 `node_ids`（对持久化节点记录做等值匹配，不区分 lease 状态——不可达节点由 rollout 既有健康门控处理），之后走与显式列表完全相同的校验/审批/持久化路径：

- `node_ids` 与 `target_selector` 同时给出 → 400（互斥）；都缺 → 既有空目标错误。
- 解析为空集 → 400 显式错误，不落任何 rollout/版本关联。
- 审批路径（`hub/approval.rs:28` 收 `node_ids`）天然钉住解析后的节点集——提交时解析即满足 spec 的"审批钉显式集"要求，无需改审批存储。
- rollout 目标表不加 selector 字段（`RolloutTargetRecord` 已是显式节点集快照）；API 响应中的 `node_ids` 即解析结果，Console 回显它。

这样 rollout 编排、审批、批次、回滚全部零改动——选择器只是目标选择的另一种入口语法。

**CR 三轮修正（CodeRabbit major）**：解析最初只读内存注册表——Hub 重启后、Agent 重注册前，有持久化标签的节点会从解析中消失，与本节"持久化节点记录"的表述矛盾。已改为经 `list_node_labels`（双后端 `SELECT node_id, labels_json FROM cp_nodes`）读取持久化记录解析；直建 rollout 路径本就不按注册状态过滤显式 node_ids，语义一致。

### D5. 列表过滤：单 `label` 查询参数，逗号连接等值 AND

`GET /nodes?label=env%3Dprod%2Ctier%3Dedge`（K8s 选择器风格）：单个参数内逗号分隔多个 `key=value` 项，AND 语义；标签 key/value 字符集不含逗号，切分无歧义。缺 `=` 或非法字符 → 400。选择单参数而非重复参数的原因：axum `Query` 提取器（serde_urlencoded）对重复键收集进 `Vec` 的行为不可依赖，且仓库既有 query 结构全部是 `Option<String>`。仅 Hub 面（本地面无 fleet 列表）。过滤在 Hub 内存中完成（≤256 节点），不动存储查询层。

### D6. 校验常量集中 + 共享校验函数

`MAX_LABELS_PER_NODE = 16`、`MAX_LABEL_KEY_LEN = MAX_LABEL_VALUE_LEN = 63`、正则 `[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?`；校验函数放 `hub/nodes.rs`（与 `sanitize_capabilities` 同处），API 层与选择器解析共用，错误信息指明违规项。

## Risks / Trade-offs

- [迁移风险：存量库 ALTER 失败] → 常量默认值 DDL 两后端均为既验证模式；迁移测试纳入双后端契约门（SQLite 全量 + PG `ARKFLOW_TEST_POSTGRES_URL` 门控）。
- [选择器匹配到离线节点导致 rollout 卡批次] → 与显式列表选了离线节点行为一致（健康门控/暂停语义已覆盖）；文档写明"选择器匹配的是持久化节点记录，不感知在线状态"。
- [提交与审批之间的时间窗内新增节点不进目标集] → 不可变快照是 spec 明确语义（防"审批时目标偷换"）；需要纳入新节点就重新提交，文档写明。
- [全量替换误清标签] → Console 编辑器提交前 diff 预览/确认；审计记录前后状态可追。
- [`labels_json` 与内存态不同步（FIFO actor 写栅栏）] → 走既有 storage actor 单写路径，无新并发面。
- [实现者把 labels 加进 `NodeMutation`/upsert SQL → register 冲掉标签] → design D2 红线写死 + 回归测试钉住（先设标签再走 register upsert，断言标签不变）。

## Migration Plan

1. 代码合入即自动迁移（启动时 schema 版本推进，幂等可重入）。
2. 回滚：schema 版本守卫会在启动时拒绝"库比二进制新"（SQLite `user_version`、PG `cp_schema_meta` 同样如此），旧版本二进制根本到不了正常读路径——受支持的回滚是**恢复迁移前的数据库备份**；手工重置版本号只是绕过守卫、既不删除 `labels_json` 列也不是通用流程。
3. 灰度顺序：先 Hub 后 Console（Console 只在 API 存在时显示标签 UI——按 `isHub` 既有判别）。

## Open Questions

无阻塞项。后续 change（`add-approval-label-scoping`）将在此数据模型上定义 `approval_required` 的标签分组策略；届时若需要"提交时校验 selector 与策略组一致"，以本 change 的"提交时解析"为锚点。
