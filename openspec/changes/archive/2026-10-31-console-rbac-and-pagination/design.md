# Design: console-rbac-and-pagination

## Context

Console（`console/`，React + Vite SPA）同时服务两种控制面：Hub 模式（`arkflow-server` Hub 面，完整路由）与本地模式（引擎二进制的 standalone 面，路由子集）。后端 RBAC（三角色×七动作）、列表分页/过滤参数、`/system` capabilities 判据均已就位；缺口全部在 Console 消费侧，外加一个身份回显端点（静态 token 会话下前端无渠道得知自身角色，见 proposal Why-1）。

既有模式约束：过滤/选中状态已走 URL query（`node_id`，`app.tsx:154+`）；数据层 React Query（`queries.ts`，`['live', …]` key + SSE 失效）；i18n zh 字典 `satisfies Record<keyof typeof en, string>` 缺 key 编译期报错；本地/Hub 同一 SPA 静态资源。

## Goals / Non-Goals

**Goals:**

- Viewer/Operator 会话不再看到（或点出）无权限的变更入口；身份可见。
- 事件/审计列表全量可达（服务端分页+过滤），状态可分享、可后退。
- 本地模式下导航不再暴露 Hub-only 死页面。
- 前端权限判定与后端**零漂移**。

**Non-Goals:**（同 proposal Non-goals）

## Decisions

### D1. 身份回显：`/system` 增字段，且回显 `actions` 而非仅 `roles`

`/system`（Hub 面 `hub_system` 与本地面 `system`）响应新增可选 `principal: { id, roles, actions }`。选 `/system` 而非新 `/whoami`：Console 启动必拉、不加路由、两面一次改齐。

关键选择：**回显后端算好的 `actions` 列表**（`OperatorPrincipal::can(action)` 逐动作求值后序列化），而不是让前端镜像角色→动作矩阵。前端门控只需 `principal.actions.includes(action)`，角色矩阵的唯一事实源留在 `api_contract.rs`——零漂移，无需快照对齐测试（proposal 初想的「镜像+快照」方案被此设计否掉）。`roles` 仍随附，仅用于身份展示（「operator (viewer)」）。

细节：principal 取自当前请求的认证中间件解析结果（Hub 面 `operator_principal(bearer)`；本地面 `local_auth_middleware` 已解析的主体经 request extension 传递）；`OperatorAction` 已 `Serialize`（snake_case）。未认证/放行场景（`insecure_local` 的 `legacy_operator()`）照实回显 Admin。旧后端无此字段——见 D5 兼容矩阵。

### D2. 门控形态：隐藏或禁用，不静默

- 变更按钮：`can(action)` 为 false 时**禁用**（不隐藏——让 Viewer 知道功能存在但需权限），`title`/tooltip 说明所需动作。
- 403 仍可能发生（时序：角色在会话中途变化、资源 scope 细粒度拒绝）：统一在 api 层识别 403 problem code → 权限错误文案 + 不自动重试（推广现有 `Configuration permission failure` spec 场景的语义到全部变更入口）。
- `canMutate`（`app.tsx:146-147`）语义扩展为 `nodeReachable && can(action)`，调用方逐处落到具体动作。

### D3. 分页/过滤：URL 为唯一事实源，服务端参数为准

过滤与分页状态全部进 URL query（与 `node_id` 同一模式），React Query `queryKey` 含全部参数、`placeholderData: keepPreviousData` 防翻页闪烁。页大小 25/50/100 三档（后端 clamp 1..100，`events.rs:31`）。

过滤控件绑定**服务端参数**（Events：`event_type`/`stream_id`/`correlation_id`/`node_id`；Audit：`resource_id`；Operations 展示处：`operation`/`state`/`correlation_id`/`node_id`），移除现有任意文本 contains 过滤——服务端无文本匹配契约，混合两种过滤语义会让「第 51 条起不可见」问题以新形态残留（文本命中在未拉取页上）。

### D4. 模式感知导航：capabilities 判据 + 路由守卫空态

判据：`/system.capabilities` 含 `fleet_aggregation`（Hub 面声明于 `nodes.rs:37`；本地面只声明 `stream_runtime`/`local_configuration`）⇒ Hub 模式。派生单一 `hubMode` 布尔量供导航与页面消费。

本地模式下：导航隐藏 Jobs/Rollouts/Approvals/Audit 四项；Settings 保留但隐藏 Webhook 管理区（#1338 落的 Hub-only 功能）。URL 直达被隐藏页时不重定向，渲染专用「仅 Hub 模式可用」解释态——比静默 302 诚实，也避免 capabilities 尚未加载时的错误跳转（capabilities 未决时导航按现状渲染，避免启动闪变）。

### D5. 兼容矩阵（前后端独立部署）

| 后端 \ 前端 | 旧 console | 新 console |
|---|---|---|
| 旧（无 principal 字段） | 现状 | 分页/导航增强全部生效（不依赖新字段）；角色门控**宽松退化**（`principal` 缺失 ⇒ 不门控、身份显示「未知」），绝不因字段缺失禁用一切 |
| 新 | 多一个可选字段被忽略，无影响 | 完整 |

部署/回滚无顺序耦合、无迁移；回滚即回退静态资源/二进制。

## Risks / Trade-offs

- [principal 字段把身份信息暴露给未认证调用方] → 字段只含 `id`/`roles`/`actions`，不含 token/scope 明文；与现有审计 actor 同级信息量；未认证请求回显 `null`。
- [actions 列表是会话快照，角色中途变更后前端过时] → 变更动作真实执行仍被后端拦（403 路径保留，D2）；导航级刷新即重拉 `/system`。
- [移除文本过滤是功能面收缩，用户可能依赖] → 服务端结构化过滤覆盖主要排查路径（按类型/流/关联 ID/节点）；release note 说明。
- [本地模式隐藏页面后，「隐藏页 URL 的书签」行为变化] → 解释态页面（D4）而非 404，书签不破坏。
- [i18n 新文案遗漏 zh] → `satisfies` 编译期兜底（既有机制）。

## Migration Plan

纯增量、无迁移。合入即生效：旧后端上新 console 只激活分页/导航两线，角色门控在新后端就位后自动激活（D5 矩阵）。

## Open Questions

（无——页大小三档、判据 capability 选 `fleet_aggregation`、文本过滤移除均已拍板，如评审有异议在 tasks 实施前修订本设计。）
