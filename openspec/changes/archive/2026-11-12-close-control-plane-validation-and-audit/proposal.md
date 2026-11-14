## Why

已归档的暂停/恢复与多用户变更仍明确保留真实双 Agent 验证、本地持久 Hub 重启验证及 PostgreSQL 选主后的吊销验证（`openspec/changes/archive/2026-11-01-add-job-pause-resume/tasks.md:67`、`openspec/changes/archive/2026-11-01-add-multi-user-credential-management/tasks.md:99`）。Job 派发审计仍将 actor 固定为 `operator`（`crates/arkflow-server/src/hub/observability.rs:296`），`GET /audit` 忽略已认证身份且仅支持 resource_id 过滤（`crates/arkflow-server/src/api/events.rs:142`）；需要补齐验证并兑现既有身份与审计契约。

## What Changes

- 给 Job 操作审计传递真实发起人；后台 reconciliation 使用明确的系统身份，不再假称操作员。操作元数据在持久化、恢复及重试中保留身份，不传播凭证。
- **BREAKING**：`GET /audit` 强制既有 `ReadAudit` 动作及资源 scope；operator 角色没有该动作，访问将返回 403。admin/viewer 保持其既定权限。
- 审计查询增加 actor、action、开始/结束毫秒时间过滤；过滤先于分页，非法时间范围稳定返回 400。
- 用真实 Hub、Agent、内核和本地 checkpoint 存储覆盖两种恢复策略的 pause/resume，补齐持久 Hub 重启恢复验证。
- 用真实 PostgreSQL 共享存储覆盖凭证吊销后的 leader 切换，以及旧 epoch 身份写拒绝。
- 同步英文/中文 API 与运维文档，并以实际测试结果更新旧归档任务及路线图。

## Non-goals

- 不增加角色、自定义权限、租户隔离或审计导出服务。
- 不改数据面 acknowledgement、checkpoint 协议或投递保证；端到端测试发现的直接阻塞缺陷在既有语义内修复。
- 不实施 server OTel、Console 图表或其他路线图功能。
- 不提交、推送或创建 PR。

## Capabilities

### New Capabilities

无。

### Modified Capabilities

- `control-plane-identity`: Job 操作的实际发起人及后台系统身份；审计读取的权限、scope 和过滤契约。

暂停/恢复和 HA 吊销的行为已有 `job-pause-resume`、`control-plane-credentials`、`hub-ha` 规格，本次补测试证据，不另造行为要求。

## Impact

- `crates/arkflow-server/src/api/{events,jobs,mod}.rs`：审计授权、过滤与请求身份接线。
- `crates/arkflow-server/src/hub/`：Job 派发审计、操作元数据及持久化恢复、重试。
- `crates/arkflow-server/tests/two_node_job_smoke.rs`、身份/选主测试：真实端到端回归。
- `docs/docs/` 与 zh-Hans 对应树、CHANGELOG、OpenSpec 任务状态和 PLANNING。
- 尽量复用现有依赖、持久化格式与测试 harness；操作身份字段为 serde 默认的可选增量字段。
