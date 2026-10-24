# Proposal: add-config-approval-flow（配置发布审批闸门）

## Why

多节点分批发布已完整落地：Hub 侧 `crates/arkflow-server/src/hub/rollout.rs:1`（"Node configuration rollouts: staged application, pause/resume/rollback"，591 行状态机，spec 见 `openspec/specs/control-plane-rollout/spec.md`——不可变版本、有界批次、健康门控、暂停/回滚、代数围栏、全程审计），Console 侧 `console/src/features/rollouts.tsx:32` 已有创建/暂停/回滚的完整 UI。但整条链路**没有任何人工审批环节**：任何持有 rollout scope 的 principal 提交即执行——多节点配置变更（可能影响全部 Stream）缺少企业变更管理要求的"第二人把关"。全仓 grep `approval|approve|审批` 在 `arkflow-server` 源码零命中，`configuration-management` spec 的 9 条 Requirement 亦无审批概念；PLANNING 5.2-5 将其列为阶段 3 剩余项。RBAC 前置已就绪（`control-plane-identity` spec 的 resource-scoped RBAC 已含 rollout scope，OIDC 用户经 claims 进同一模型），审批流是"进企业生产"叙事的最后一块运营拼图。

## What Changes

- **新增审批请求资源与状态机**：`PENDING → APPROVED / REJECTED / EXPIRED`。审批对象是不可变快照 `(config_version_id, targets, batch_size)`——config version 本身不可变，批准后草案再改产生新版本 id，旧批准不可能泄漏到新内容。
- **Hub 策略开关**：`approval_required`（默认 `false`，显式开启）。开启后 rollout 的**全部四个创建入口**——批量创建（`api/rollouts.rs:33`）、带内容创建、单节点 legacy apply（`api/configuration.rs:210`）、回滚创建（单节点回滚 `api/configuration.rs:220` 与 rollout 动作回滚 `rollout.rs:341`）——封堵未批内容，直接创建被显式拒绝并指向审批流。**回滚仅在目标版本"曾应用到目标节点"时免审放行**：现行实现接受客户端自选的任意版本 id 且不查历史（`request.config_version` 原样透传，`rollout.rs:333` 仅查在场），无此约束则"未批版本借道回滚"就是绕闸通道。关闭时行为零变化。启动期在"开启策略且静态 operator 身份 < 2"时告警——单身份部署开启即冻结发布（自批被职责分离拒绝），必须显式知情。
- **审批 API**：提交待审请求、列出（按状态过滤）、批准、拒绝。批准经 `APPROVING` 中间态 + correlation id 认领保证崩溃后不双建 rollout；批准后由系统代为创建 rollout（actor 记审批人，correlation id 链接请求 → rollout），既有 rollout 状态机、reconciliation、Intent/Convergence 语义**一行不动**。
- **职责分离**：新增 RBAC action；`approval_required` 开启时审批人 principal 必须不同于提交人 principal。
- **持久化与生命周期**：新表 + SQLite/PG 双后端实现，审批状态跨 Hub 重启存活（`repair-hub-restart-lifecycle-wedge` 的教训）；待审请求带 TTL（`expires_at_ms`，复用 job 命令幂等元数据先例），过期惰性判定 + 周期清扫接入既有 retention 任务。
- **审计**：submit/approve/reject 均落审计事件（actor、目标、correlation、结果），照 `add-hub-job-audit-and-command-metrics` 先例，不含凭据与配置正文。
- **REJECTED 为终态**：不"退回修改"；重新提交走新请求，Console 从被拒请求预填表单（操作顺畅性在 UI 层解决，审计记录不可变形）。
- **Console**：审批队列页（待审/已决列表 + 批准/拒绝操作）+ 发布流程插入"提交待审"分支 + 被拒请求一键预填重提。

## Non-goals

- **按节点标签分组的审批策略**（如 `env=prod` 组强制审批）——节点标签体系是阶段 3 尚未落地的基础设施，存在依赖倒挂；标签落地后另行扩展。
- **多级审批/会签/仲裁**（quorum、可配置审批流水线）——单人审批已覆盖企业变更管理的"第二人把关"，流程引擎属过度设计。
- **GitOps 同步本体**——本变更只保证闸门对"任何提交通道"生效的语义可组合；Git 同步本身另有触发条件（见 PLANNING 7.5）。
- **修改 rollout/reconciliation 内部语义**——审批是入口闸门，`control-plane-reconciliation` 的 generation/Intent/Convergence 不容第二处实现。
- **Console 多人协作草案编辑**——草案编辑工作流（draft/validate/diff）保持现状。
- **节点本地控制面的治理**——`cp.apply_configuration` 的本地路径（非 Hub 控制面，`api/configuration.rs:391`）属独立信任域，与今天所有 Hub 策略的边界一致；审批只治理 Hub 中介的发布。

## Capabilities

### New Capabilities

- `configuration-approval`: 配置发布审批闸门——审批请求状态机、策略开关、职责分离、TTL 过期、持久化与审计契约。

### Modified Capabilities

- `control-plane-rollout`: "Create an immutable configuration rollout" 需求增加策略门控分支——`approval_required` 开启时 rollout 创建（含单节点 legacy apply 复用路径）必须经审批请求批准后由系统发起，直接创建被显式拒绝。

## Impact

- `crates/arkflow-server/src/`：`hub/`（新审批状态机模块）、`api/`（审批路由 + rollouts 路由的门控分支）、`storage/`（新表 + StorageBackend trait 方法 + SQLite/PG 双实现 + schema 版本 bump + migrate 子命令契约）、`api_contract.rs`（新 OperatorAction）。
- `console/`：审批队列页、rollouts 页提交流程分支、被拒预填重提。
- 文档：`docs/docs/control-plane/http-api-v1.md` 审批路由、配置管理页 en/zh、CHANGELOG。
- 依赖：零新增。
- 兼容性：默认关闭开关 = 现有部署行为逐字节不变；开启属运维显式动作。
