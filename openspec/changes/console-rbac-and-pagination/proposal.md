# Proposal: console-rbac-and-pagination

## Why

Console 是控制面唯一的操作入口，但三处「后端契约已就位、前端未消费」的缺口让它对真实部署不完整或不诚实（PLANNING.md 11.3 盘点，本次立项前已逐条核实）：

1. **角色感知为零**。后端 RBAC 完备：三角色（Admin/Operator/Viewer）× 八动作的 `can()` 映射在 `api_contract.rs:63-80`，静态结构化凭证携带角色（`hub/operator.rs:27-31`），所有 Hub 面变更端点按动作鉴权。但前端 `canMutate` 只反映节点是否在线（`console/src/app.tsx:146-147`）：Viewer 会话看到全部可变更按钮、点击后才 403；身份无处显示。更根本的：静态 token 会话下 `/auth/oidc/status` 恒返回 `principal: null`（`console/src/api.ts:819-821`），前端**无从得知**自身角色——纯前端方案对静态 token 部署（当前主要部署形态）不可行，需要一个身份回显端点。
2. **列表数据第 51 条起不可见**。后端 `EventQuery`/`AuditQuery`/`OperationQuery` 分页与过滤参数齐全（`api/mod.rs:347-373`），默认页大小 50、上限 100（`events.rs:31`）；前端一个分页参数都不传（`console/src/api.ts:468-471`、`621-622`）。Events 页对首屏 50 条做客户端过滤、并明示「还有更多未显示」（`features/events.tsx:35-36`）；Audit 页 `staleTime: Infinity` 拉一次第 1 页永不刷新（`features/audit.tsx:10`）。事件多于 50 条的部署，第 51 条起在 UI 中永久不可达。
3. **本地模式死导航**。导航 10 项无条件渲染（`console/src/app.tsx:47-56`），其中 Jobs/Rollouts/Approvals/Audit 四页的后端路由只存在于 Hub 面（本地 router `api/mod.rs:409-434` 无对应路由），Settings 页的 Webhook 管理区同样 Hub-only——连本地引擎时这些页面空转报错。两面 `/system` 已声明可区分的 capabilities（Hub：`node_registry`/`command_dispatch`/`fleet_aggregation`，`api/nodes.rs:37`；本地：`stream_runtime`/`local_configuration`，`control_plane.rs:161`），判据现成。

## What Changes

- **身份回显（唯一后端改动，纯增量）**：`/system` 响应（Hub 面与本地面）新增可选 `principal: { id, roles }` 字段，回显当前请求已认证的操作员身份——静态 token 与 OIDC 会话统一。
- **角色感知 UI**：Console 引入与后端 `OperatorPrincipal::can()` 一致的判定（前端镜像三角色×七动作映射），门控全部变更入口（流生命周期、配置发布/回滚、Job 操作、Rollout 创建、审批决定、节点维护、Webhook 管理）；顶栏显示当前身份（principal id + 角色）；403 统一为友好降级（说明无权限、不自动重试）。
- **服务端分页/过滤**：Events/Audit（及 operations 列表展示处）改为透传既有 query 参数（`event_type`/`stream_id`/`correlation_id`/`node_id`、`resource_id`、`operation`/`state` 等）+ 页码控件，过滤与分页状态跟随 URL；移除客户端过滤。
- **模式感知导航**：按 `/system` capabilities 门控导航与路由可达性——本地模式隐藏 Jobs/Rollouts/Approvals/Audit 四页；Settings 保留但隐藏其 Hub-only 的 Webhook 管理区。

## Non-goals

- 不改后端 RBAC 判定逻辑、角色模型，不新增角色或动作（`/system` 只回显已解析的主体，不重定义权限）。
- 不做用户/凭证管理面与 JWT 吊销（P3 多用户方向）。
- 不新增列表端点或聚合端点、不改分页参数契约——只消费既有 `EventQuery`/`AuditQuery`/`OperationQuery`。
- 不做监控时序图表、per-node 指标下钻、token 运行时输入、rollout 版本选择器（11.4 P2「Console 监控增强」，另行立项）。
- 不给本地 standalone 面补齐 Hub 等价路由（死页面以隐藏方式解决，不以扩后端方式解决）。

## Capabilities

### New Capabilities

（无——全部落在既有能力的 delta 上）

### Modified Capabilities

- `control-plane-console`: 新增角色感知 UI 需求（身份显示、按动作门控、403 降级）；`Components, events, and settings` 需求的分页语义从客户端过滤改为服务端分页/过滤；`Operations application shell` 需求增加 capabilities 驱动的模式感知导航（本地模式不渲染 Hub-only 页面入口）；`Runtime administration` 需求的变更禁用条件从「节点不可用」扩展为「节点不可用或主体无权限」。
- `control-plane-api`: `/system` 响应新增认证主体回显字段（增量、可选，两面一致）。

## Impact

- **后端（小）**：`crates/arkflow-server/src/api/nodes.rs`（`hub_system`）、本地面 `system` 处理器及 `OperatorPrincipal` 序列化；契约测试更新。无存储/迁移/协议兼容影响（字段可选、纯新增）。
- **前端（主体）**：`console/src/api.ts`（列表函数签名带分页/过滤参数、`SystemResource.principal` 类型）、`app.tsx`（导航门控、`canMutate` 纳入角色）、`queries.ts`（分页 query key）、`features/` 下 events/audit/runtime/settings/jobs/rollouts/approvals/configuration 各页（门控 + 分页控件）、`i18n/`（en+zh 新文案，zh 字典缺 key 编译期报错）。
- **防漂移**：角色×动作矩阵由后端契约测试序列化为快照，前端测试断言与快照一致（防止前端镜像与 `api_contract.rs` 静默漂移）。
- **文档**：`docs/` 控制台/控制面相关页面（en + zh-Hans 两树）涉及 RBAC 展示、分页行为、本地模式导航的段落需同步。
