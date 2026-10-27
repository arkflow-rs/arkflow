# add-node-labels — Tasks

## 1. 存储层：labels 列与迁移

- [x] 1.1 `storage/mod.rs`：`NodeRecord` 增 `labels: BTreeMap<String, String>`（serde default），行映射按 `capabilities_json` 同法编解码 `labels_json`
- [x] 1.2 SQLite：`PRAGMA table_info` 守卫 + 条件 `ALTER TABLE cp_nodes ADD COLUMN labels_json TEXT NOT NULL DEFAULT '{}'`（沿用 `node_version`/`maintenance_state` 加列先例）；新建库 `CREATE TABLE cp_nodes` 同步加列；迁移测试覆盖存量库升级与幂等重入
- [x] 1.3 Postgres：`cp_schema_meta` 版本 bump + `ADD COLUMN IF NOT EXISTS`；`ARKFLOW_TEST_POSTGRES_URL` 门控契约测试覆盖迁移与标签读写
- [x] 1.4 **保持 `upsert_node` 的 INSERT/ON CONFLICT 列清单不含 labels_json**（`NodeMutation` 不加字段，靠列 DEFAULT 与显式列集不触碰，见 design D2 红线）；`replace_node_labels` 走独立 UPDATE 专用路径；回归测试：先 PUT labels → 模拟 register/heartbeat upsert → 断言标签不变；无标签时现有行为逐位一致

## 2. Hub 域逻辑

- [x] 2.1 `hub/nodes.rs`：`validate_labels`（≤16 条、key/value ≤63 字符、`[A-Za-z0-9]([A-Za-z0-9_.-]*[A-Za-z0-9])?`，错误信息指明违规项）+ 常量；单测覆盖空 key、64 字符值、首尾 `-_.`、17 条、合法通过
- [x] 2.2 `replace_node_labels` 域方法：校验 → 存储全量替换 → 审计（`node_labels_replace`，接受与拒绝均落审计）；未知节点返回 not-found；单测
- [x] 2.3 节点列表标签过滤：重复 `label` 参数 `key=value` AND 语义，非法表达式显式错误；单测
- [x] 2.4 `resolve_label_selector`：对持久化节点记录等值匹配（不感知 lease 状态），返回有序去重 node_ids；单测含空集与多标签 AND

## 3. API 层

- [x] 3.1 `PUT /nodes/{id}/labels` 路由：`ManageNodes` + node scope 判定，403 拒绝落审计；hub_router 注册；节点表示（`GET /nodes`、`/nodes/{id}`、fleet status）携带 labels
- [x] 3.2 `GET /nodes` 标签过滤参数解析接线（Hub 面）
- [x] 3.3 `CreateRolloutRequest` 增 `target_selector`（serde default）：与 `node_ids` 互斥校验（同给/都缺显式 400）+ 空集拒绝；解析后进入既有 node_ids 路径（直建与审批提交两个入口统一前置解析）
- [x] 3.4 API 集成测试：selector 创建 rollout 落显式目标并在响应回显、创建后改标签不改目标集、审批开启时提交钉解析集（approve 后目标一致）、RBAC 拒绝矩阵、404/400 错误形态

## 4. Console

- [x] 4.1 `api.ts`：labels 字段类型、PUT labels、label 过滤参数、rollout `target_selector` 请求字段
- [x] 4.2 `overview.tsx` 节点卡片：标签展示/编辑（全量替换提交 + 确认对话框，复用既有 AlertDialog 模式）
- [x] 4.3 `rollouts.tsx` 创建表单：显式勾选与标签选择双模式（互斥切换）；标签模式提交前经 `GET /nodes?label=` 预览解析节点数
- [x] 4.4 i18n en/zh 字典补全；console 构建/测试/lint 全绿

## 5. 文档（en/zh 同步）

- [x] 5.1 `docs/docs/control-plane/http-api-v1.md`：PUT labels、label 过滤、`target_selector` 字段说明（en+zh）
- [x] 5.2 多节点运营/rollout 文档：标签语义与边界（匹配不感知在线状态、审批钉快照、创建后变更不改目标）（en+zh）
- [x] 5.3 新增代码块按文档校验规则标注 fence 标记；`pnpm docs:check` 通过

## 6. 验证收尾

- [x] 6.1 `cargo test --workspace --all-targets` 全绿、`cargo clippy --workspace --all-targets` 无新增告警
- [x] 6.2 真实运行验证：Hub（sqlite）+ 注册 agent，Console 打标签 → 按 selector 创建 rollout → 观察收敛与目标集不变性；自我 CR 后浏览器实跑核对语义
- [x] 6.3 `openspec validate add-node-labels` 通过；归档前 delta specs 与实现逐条核对
