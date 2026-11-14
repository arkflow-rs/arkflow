## Context

收尾包含三个可独立验证的部分：真实暂停/恢复、PostgreSQL HA 身份一致性、审计权限与身份。测试沿用现有 Hub/Agent 与 storage actor，不引入模拟运行时。身份审计漏斗已经实现，本次只处理 Job 操作的 actor 缺口。

## Goals / Non-Goals

**Goals:**
- 操作员触发的 Job 操作携带真实 principal id；后台工作使用 `system`；已记录操作的 actor 在恢复与重试中不丢失。
- 审计读取落实现有角色与 scope，并在分页前组合过滤。
- 把旧变更留下的端到端验证转成可重复执行的回归测试，并实际运行。

**Non-Goals:**
- 不增加角色、配额、租户、审计导出或 Console 监控 UI。
- 不改变内核的投递/恢复契约，不用 mock manifest 冒充真实内核验证。

## Decisions

### 1. 显式传递 Job 操作身份

API 已解析 principal，沿 Job 变更、placement 与 enqueue 的直接调用显式传递其 id。保留内部无 actor 的入口，使后台 reconciliation 使用 `system`。`HubOperation` 增加 serde default 的可选 actor 字段；持久化本来就是 operation JSON，无需新建表。恢复/重试应保留已有 actor；历史无 actor 操作按系统来源处理。凭证本身不进入操作或命令 payload。避免 task-local 身份上下文，防止异步派发在离开 HTTP future 后丢失来源。

### 2. 审计授权使用既有模型

`hub_audit` 先调用既有授权助手要求 `ReadAudit`。资源查询按 `audit` scope 检查，带具体 resource_id 的请求按该 id 检查；无过滤查询要求可访问整个审计集合。保持 admin/operator/viewer 的现有角色矩阵，不为兼容无权限的旧访问放宽角色。

`AuditQuery` 增加可选 `actor`、`action`、`from_ms`、`to_ms`。时间为包含端点的 Unix 毫秒；from > to 返回稳定 `invalid_audit_query` 400。实现中核实双后端 `list_audit` 都先执行 `LIMIT 1024`，因此仅在 handler 过滤会让旧匹配记录被较新的无关记录遮挡。新增 `AuditFilter` 与兼容的 `list_audit_filtered` 读取入口，经现有 storage_ops 委托到两后端的参数化 SQL，先组合 WHERE 条件，再应用 1024 条返回上界，最后由 handler 分页。旧 `list_audit` 入口、默认页大小与上限保留。额外过滤不扩大授权范围。

### 3. 真实暂停/恢复验证

扩展 `two_node_job_smoke`：使用真实 Agent 和 kernel、临时目录 `file://` checkpoint 与 SQLite Hub；分别覆盖 latest_checkpoint 与 latest_savepoint。断言两节点均真实运行/停止、pause 工件类型与 manifest 存在、resume 启动命令选中该工件，并证明恢复后数据继续推进。加入 Hub 重启后再次 pause/resume 的持久化场景。使用条件等待和有界 teardown，避免固定长 sleep 与泄漏进程。

### 4. 真实 PostgreSQL HA 验证

测试遵循 `ARKFLOW_TEST_POSTGRES_URL` 的现有门控约定，使用隔离 schema/数据库及两个独立 store handle。经真实 lease 获取和释放完成 leader 切换；通过 HTTP 证明吊销前可认证、吊销后与新 leader 下均 401。保留旧 epoch 的写路径应被 WriteFence 拒绝，且新 leader 下身份状态未改变。运行时提供真实 PostgreSQL，不把缺少环境导致的跳过报告为通过。

## Risks / Trade-offs

- [operator 角色以前可读审计] → 文档明确现有 ReadAudit 矩阵开始执行；测试覆盖 admin/viewer 成功、operator 403 和 scope 拒绝。
- [恢复测试发现直接行为缺陷] → 只修复阻塞本次既有契约的代码，并添加确定性回归。
- [集成测试负载与资源泄漏] → 复用现有串行锁、临时目录、条件等待和有界 shutdown。
- [外部 PostgreSQL 不可用] → 优先复用现有本地服务或临时测试容器；若受环境限制，准确记录阻塞，不勾掉未验证任务。

## Migration Plan

操作 JSON 新字段可选，旧行仍可读取；升级不要求 schema bump。部署前确认需要读取审计的用户具有 admin 或 viewer 角色以及相应 scope。回退旧二进制会忽略新增操作 JSON 字段；回退也会失去本次审计权限检查。

## Open Questions

无待用户决策项；测试发现的实现细节按既有规格收敛。
