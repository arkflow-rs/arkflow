# Design: add-config-approval-flow（配置发布审批闸门）

## Context

Rollout 机制已完备：`hub/rollout.rs` 的 `create_rollout`（:11）/`create_rollout_with_content`（:91）经 `api/rollouts.rs` 暴露，Console 有完整 UI（`features/rollouts.tsx`）。创建入口共**四处**：批量创建（`api/rollouts.rs:33`）、单节点 apply（`api/configuration.rs:210` → `create_rollout_with_content`）、单节点回滚（`api/configuration.rs:220` → `create_rollout`）、rollout 动作回滚（`rollout.rs:341`）。**回滚目标版本是客户端自选的**：`request.config_version` 原样透传至 `create_rollout`，现行实现只查"版本存在"不查"曾应用过"（`rollout.rs:333` 仅查在场）——策略开启后若无约束，这是绕闸通道。另注意 `api/configuration.rs:391` 的 `cp.apply_configuration` 是节点本地（非 Hub）控制面路径，与 Hub 审批属不同信任域（Non-goal）。

可骑乘的既有设施：`OperatorAction` 枚举（`api_contract.rs:34`——Read/Operate/Configure/ManageNodes/ManageRollouts/ReadAudit，Admin 全量、Operator 除 ReadAudit、Viewer 只读）、`record_audit_event`（`hub/observability.rs:259`）、`storage_ops!` 宏（`storage/mod.rs:446`，加方法 = 宏表条目 + SQLite/PG 双实现）、60s 周期维护任务（`bound-control-plane-storage-history` 引入，retention 清扫所在地）、`HubConfig`（`hub.rs:63`）。

## Goals / Non-Goals

**Goals:**

- `approval_required` 开启时，全部四个 rollout 创建路径（批量/带内容/单节点 apply/回滚）封堵未批内容；回滚仅放行"曾应用到目标节点"的版本（D2）。
- 审批对象不可变：`(config_version_id, targets, batch_size)` 快照；批准即由系统代建 rollout（actor=审批人）。
- 职责分离：审批人 principal ≠ 提交人 principal；审批是高权限动作。
- 状态跨 Hub 重启存活；待审请求 TTL 过期；终态请求有界保留。
- 默认关闭 = 现有行为零变化。

**Non-Goals:**

- 标签分组策略、多级审批/会签、GitOps 同步本体、rollout/reconciliation 状态机内部语义修改（见 proposal Non-goals）。
- 回滚动作的全量过闸与无条件豁免（取"曾应用版本"约束，见 D2）；节点本地控制面路径的治理（非 Hub 信任域）。

## Decisions

### D1：审批是独立一等资源，不是 rollout 的前置状态

新表 `cp_config_approval_requests`；不往 rollout 状态机里插 `PendingApproval` 态。理由：rollout 是不可变的执行记录，reconciliation 语义（`control-plane-reconciliation`）不容第二处实现；闸门放入口使 rollout 内部零改动。备选（否决）：rollout 增加 pending 分支——会触碰批次推进/健康门控的既有判断，且"审批"与"执行"生命周期不同（请求可过期、可重提，rollout 不可变）。

### D2：闸门在 API 层、封堵全部四个创建入口；回滚只放行"曾应用版本"

`approval_required: bool`（`HubConfig` 新字段，serde 默认 `false`）+ `approval_ttl_ms`（默认 24h）。开启后四个创建入口统一走 `ensure_rollout_creation_allowed`：未过闸 → 显式错误（文案指向 `POST /config-approvals`），零持久化副作用。**回滚约束（而非无条件豁免）**：两处回滚创建路径在策略开启时校验目标版本**曾应用到目标节点**（存在以该 `config_version_id` 应用/收敛过的 rollout target 记录）——曾应用 = 曾过闸（或策略开启前上线），放行；指向从未应用的版本 = 未批内容借道，拒绝并提示走审批。历史被 retention 清扫致查无记录时 fail-closed（对 genuinely 旧版本多走一步审批，方向安全）。策略关闭时回滚保持现状（接受任意存在的版本 id，不新增校验）。

**策略关闭时的审批语义**：submit 显式拒绝（"审批流未启用"）；已存在的 pending 请求仍可 decide——直接创建本已放开，批准不增加风险面；TTL 照常。**启动校验**：`approval_required` 开启且静态配置的去重 operator 身份数 < 2 时**启动告警但不拒绝启动**（OIDC 身份运行期才出现，无法启动期穷举；凭据语法校验 fail-closed 的先例不适用于存在性判断）。备选（否决）：回滚无条件豁免——`request.config_version` 客户端自选，等于留绕闸通道；回滚全量过闸——牺牲事故响应，"曾应用版本"已提供等价安全保证；per-request 审批标志——请求方自选等于可绕过。

### D3：批准的原子序——`APPROVING` 中间态 + correlation id 认领，崩溃不双建

`decide(approve)` 流程：①CAS `PENDING → APPROVING`（败者 409；终态/过期拒绝）锁定决策权 → ②**先按请求的 correlation id 查既有 rollout**：命中即认领（崩溃恢复路径，绝不重复创建）；未命中则以审批人 actor 创建 rollout → ③请求落 `APPROVED` 并记录 `rollout_id`。②失败（目标节点已消失等——提交期已校验，属异常路径）回退 `PENDING` 记 failure_note，可重试。崩溃窗口由 `APPROVING` + 认领闭合：任何时刻至多一个 rollout 与请求关联（correlation id 唯一锚）；卡在 `APPROVING` 的请求由启动恢复与周期清扫按同一认领规则收敛（有 rollout → `APPROVED`，无 → 回 `PENDING`）。提交期校验：版本存在、targets 节点存在、batch 配置合法（复用 `create_rollout` 入参校验但不执行）。备选（否决）：`PENDING → APPROVED` 直转后再建 rollout——崩溃留下"已批准但无 rollout"孤儿态；不设中间态——崩溃后重试双建 rollout（`repair-hub-restart-lifecycle-wedge` 抓过的同类缺陷，不重蹈）。

### D4：职责分离与 RBAC——新 action `ApproveRollouts`，仅 Admin

`OperatorAction::ApproveRollouts`：Admin 允许，Operator/Viewer 拒绝（`api_contract.rs` 的 `can` match 各加一支）。提交用既有 `ManageRollouts`。审批时校验 `approver.id != requester.id`——legacy 静态 token 的 principal id 固定为 `"operator"`，因此开启策略后单管理员静态 token **无法自批自提**，这正是开启该策略的语义（需要两名运维身份；文档明示 + D2 启动告警兜底）。备选（否决）：Operator 也可批——则 Operator 同时持有提交与批准，职责分离只剩 id 检查一道纸面防线。

### D5：生命周期——TTL 惰性判定 + 周期清扫 + 终态有界保留

`expires_at_ms = created_at + approval_ttl_ms`。读取路径惰性判定（过期即按 `EXPIRED` 呈现），周期清扫接入 60s 维护任务（终态请求 30 天 + 条数上限双界，照 `cp_audit_events` 先例）；清扫非 operator mutation，不进审计。重新提交 = 新请求（新 id），REJECTED 终态永不变形。

### D6：存储——宏表加 6 方法，双后端 + schema 版本 bump

`storage_ops!` 宏表新增：`create_config_approval_request` / `get_config_approval_request` / `list_config_approval_requests(status)` / `begin_decide_config_approval_request(id, decision, approver)`（CAS `PENDING → APPROVING`/`REJECTED`） / `complete_config_approval_request(id, rollout_id)`（`APPROVING → APPROVED`）/ `reset_config_approval_request(id, failure_note)`（`APPROVING → PENDING`，创建失败回退与恢复共用）/ `find_rollout_by_correlation(correlation_id)`（认领查询）/ `sweep_config_approval_requests(now)`（过期 + retention + 卡死 `APPROVING` 归位）/ `count_config_approval_requests(status)`（运维面）。SQLite 与 Postgres 双实现、DB schema 版本号 +1、`arkflow-server migrate` 子命令契约、`ARKFLOW_TEST_POSTGRES_URL` 门控的 PG 契约测试沿用双后端测试门禁。表列：`id, config_version_id, targets(json), batch_size, requester_id, status, approver_id, decided_at, rollout_id, correlation_id, expires_at_ms, created_at, failure_note`。

### D7：API 与 Console

API（operator 路由，自动受认证中间件保护）：`POST /config-approvals`（提交）、`GET /config-approvals?status=`、`GET /config-approvals/{id}`、`POST /config-approvals/{id}/decide`（`{decision: approve|reject, note?}`）；策略可见性经既有 hub 状态端点附加 `approval_required` 字段（Console 据此切换发布按钮为"提交待审"）。Console：审批队列页（待审/已决 + 批准/拒绝）、rollouts 页按策略分流、被拒请求预填重提。审计：submit/approve/reject 三类 mutation 落 `record_audit_event`，不含配置正文。

## Risks / Trade-offs

- [批准时目标节点已消失 → rollout 创建失败] → D3 原子序：请求回退 PENDING 并记原因，提交期校验使该路径仅剩时间窗竞态。
- [批准中途崩溃 → 重试双建 rollout] → `APPROVING` 中间态 + correlation id 认领（D3）：重试/恢复先查既有 rollout，命中即认领，任何时刻至多一个 rollout 关联请求。
- [并发双审批] → CAS 只允许一个决策落定，后到者 409；审计各自记录。
- [静态 token 单管理员被策略锁死] → 启动告警（D2：策略开启且静态 operator 身份 < 2）+ 文档明示开启前提是两名以上 operator 身份；这是特性而非缺陷。
- [回滚"曾应用"校验因历史被 retention 清扫而查无记录] → fail-closed 拒绝并提示走审批（对 genuinely 旧版本多走一步审批，方向安全）。
- [待审请求洪泛] → TTL 过期 + 终态双界保留；列表按状态过滤。
- [approval_required 开启前的存量 rollout 操作] → 不追溯：闸门只管"创建"；pause/resume/cancel 照旧，rollback 按"曾应用版本"约束（D2）放行。

## Migration Plan

DB schema 版本 bump（新表，无既有表变更）+ `migrate` 子命令支持；开关默认 `false`，升级后行为零变化。回滚：旧二进制直接部署（新表残留无害）。开启策略属运维显式动作，建议先在双 operator 环境验证。

## Open Questions

（无——TTL 默认 24h 可在 review 时调整；标签分组策略待节点标签体系落地后另立 change。）
