## 1. 存储与数据模型

- [x] 1.1 定义 `ConfigApprovalRequest` 领域结构（id, config_version_id, targets, batch_size, requester_id, status, approver_id, decided_at, rollout_id, correlation_id, expires_at_ms, created_at, failure_note）与 status 枚举（Pending/Approving/Approved/Rejected/Expired），serde 双向兼容
- [x] 1.2 SQLite + Postgres 建表 `cp_config_approval_requests`（DB schema 版本号 +1），`arkflow-server migrate` 子命令覆盖新表，索引覆盖 (status, created_at)
- [x] 1.3 `storage_ops!` 宏表新增九方法：create / get / list(status) / begin_decide（CAS `Pending→Approving`/`Rejected`）/ complete（`Approving→Approved` + rollout_id）/ reset（`Approving→Pending` + failure_note）/ find_rollout_by_correlation（认领查询）/ sweep(now, age_bound, count_bound) / count(status)，双后端实现 + 存储层单测（CAS 冲突、终态不可再决、认领唯一性、sweep 不删 pending、Approving 归位）
- [x] 1.4 `ARKFLOW_TEST_POSTGRES_URL` 门控的 PG 契约测试扩展覆盖六方法（沿双后端契约测试门禁）

## 2. 策略开关与 RBAC

- [x] 2.1 `HubConfig` 新增 `approval_required: bool`（serde default false）与 `approval_ttl_ms: u64`（默认 24h），启动校验与配置文档注释
- [x] 2.2 `OperatorAction` 新增 `ApproveRollouts`：Admin 允许、Operator/Viewer 拒绝（`api_contract.rs` 的 `can` match + 既有 RBAC 矩阵测试扩展）
- [x] 2.3 hub 状态端点附加 `approval_required` 字段（Console 据此分流；serde default 向后兼容）
- [ ] 2.4 启动校验告警：`approval_required` 开启且静态配置去重 operator 身份 < 2 时输出醒目告警（不拒绝启动；单测覆盖单身份/双身份/OIDC-only 形态）

## 3. 审批状态机与 API

- [x] 3.1 `hub/` 新建审批模块：submit（校验版本存在/节点存在/batch 合法，不建 rollout 不派发）、list/get；未知引用显式报错（单测）
- [x] 3.2 decide(approve)：CAS `Pending→Approving` → 先按 correlation id 查既有 rollout（命中即认领，不重复创建）→ 未命中则以审批人 actor 调 `create_rollout`（D3 快照参数原样透传）→ 成功落 Approved + rollout_id，失败 reset 回 Pending 记 failure_note（单测：成功路径、认领路径、创建失败回退、并发 409）
- [x] 3.3 decide(reject)：落 Rejected + 审批人 + 时间戳；终态再决策显式报错（单测）；reject 不触碰快照
- [x] 3.4 职责分离：approver principal id == requester id 时 decide 失败并指名规则（单测）
- [x] 3.5 策略关闭语义：submit 显式拒绝（"审批流未启用"）；pending 请求在策略关闭后仍可 decide（单测两方向）
- [x] 3.6 API 路由：`POST /config-approvals`、`GET /config-approvals?status=`、`GET /config-approvals/{id}`、`POST /config-approvals/{id}/decide`（operator 中间件自动保护 + handler 内 ApproveRollouts/ManageRollouts scope 检查，照既有 require_operator_action 形态）
- [x] 3.7 401/403/终态/过期/CAS 冲突的错误码矩阵测试（照既有认证矩阵测试先例）

## 4. 闸门接入 rollout 创建路径

- [x] 4.1 `ensure_rollout_creation_allowed` 门控 helper；接入全部四个创建入口——`api/rollouts.rs:33` 批量创建、`api/configuration.rs:210` 单节点 apply、`api/configuration.rs:220` 单节点回滚、`rollout.rs:341` rollout 动作回滚；错误文案指向审批流（单测逐路径断言拒绝且零持久化副作用）
- [x] 4.2 回滚"曾应用版本"校验（两处回滚路径共用）：策略开启时目标版本 MUST 存在以该 `config_version_id` 应用/收敛过的 rollout target 记录，查无记录 fail-closed；策略关闭时保持现状不校验（单测：曾应用放行、从未应用拒绝、retention 清扫后 fail-closed）
- [x] 4.3 验证 pause/resume/cancel 不受闸门影响回归
- [x] 4.4 集成测试：policy off 全链路行为与现状逐位一致（默认零变化回归，含回滚路径）；policy on 下 approve 后 rollout 以审批人 actor 出现、批次推进照常收敛

## 5. 生命周期与审计

- [x] 5.1 TTL：submit 落 expires_at_ms；读取与 decide 惰性判定过期（Expired 不可批，单测）
- [x] 5.2 周期清扫接入 60s 维护任务（终态 30 天 + 条数双界；pending 永不回收；卡死 `Approving` 按认领规则归位——有 rollout 认领为 Approved、无则回 Pending；清扫不进审计——单测断言无 operator mutation 审计事件）
- [x] 5.3 submit/approve/reject 落 `record_audit_event`（actor/action/request id/version/targets/correlation/outcome；不含配置正文与凭据）；审计断言测试
- [x] 5.4 重启存活测试：pending 请求经存储重建后仍可决策；卡在 `Approving` 的请求重启后按认领规则收敛、不双建 rollout（沿 `verify-hub-production-readiness` 的 harness 形态或存储层重建测试）

## 6. Console

- [x] 6.1 API client 与 queries：审批列表/详情/提交/决策 + 状态端点 `approval_required` 读取
- [x] 6.2 审批队列页：待审/已决列表、批准/拒绝操作（带 ApproveRollouts 权限的入口）、失败原因与过期状态展示
- [x] 6.3 rollouts 页分流：policy on 时创建按钮改"提交待审"；被拒请求预填表单一键重提（新请求 id）
- [x] 6.4 Console 五件套（typecheck/lint/test/build + i18n key）通过

## 7. 文档（en + zh-Hans）

- [x] 7.1 `docs/docs/control-plane/http-api-v1.md` + zh 对应页：审批四路由、错误语义、`approval_required` 字段
- [x] 7.2 `docs/docs/how-to/5-control-plane-rollout.md` + `docs/docs/operate/control-plane/operations.md` + zh 对应页：审批流操作指南（含"开启前提是至少两名 operator 身份、单静态 token 无法自批自提"的明示）
- [x] 7.3 fence 分类标记与 `pnpm docs:check` 通过；CHANGELOG 条目

## 8. 收口验证

- [x] 8.1 `cargo test --workspace --all-targets` 全绿 + `cargo clippy --workspace --all-targets` 无新增告警
- [x] 8.2 端到端实跑验证：双 principal（admin1 提交、admin2 批准）经真实 Hub 完成 提交→直接创建被拒→批准→rollout 收敛→被拒路径→过期路径
