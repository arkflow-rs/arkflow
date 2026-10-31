# Tasks: console-rbac-and-pagination

## 1. 后端：/system 认证主体回显

- [x] 1.1 `OperatorPrincipal` 增加回显序列化（`{ id, roles, actions }`，`actions` 由 `can()` 逐动作求值），Hub 面 `hub_system`（`api/nodes.rs`）回显当前请求主体；未认证回显 `null`
- [x] 1.2 本地面 `system` 处理器以同一序列化回显（`local_auth_middleware` 解析的主体经 request extension 传递）；`insecure_local` 的 `legacy_operator()` 照实回显
- [x] 1.3 契约测试：静态凭证 viewer/operator 各角色回显的 `actions` 与 `can()` 断言一致、未认证 `null`、字段缺失不破坏旧响应解析（`cargo test -p arkflow-server`）

## 2. Console API 层与派生状态

- [x] 2.1 `SystemResource` 增加可选 `principal?: { id, roles, actions }`；派生 `hubMode`（capabilities 含 `fleet_aggregation`）与 `can(action)` 助手（principal 缺失 ⇒ 宽松模式返回 true，见 design D5）
- [x] 2.2 列表函数签名扩展：`events`/`operations`/`audit` 接受分页/过滤参数对象并序列化为 query（`page`/`page_size`/`event_type`/`stream_id`/`correlation_id`/`node_id`/`resource_id`/`operation`/`state`）
- [x] 2.3 403 识别：`request` 层将 403 problem 映射为权限错误类型（可展示所需动作、不自动重试的语义标记）

## 3. 角色感知 UI

- [x] 3.1 全部变更入口按动作门控（流 start/stop/restart→Operate、配置 publish/rollback→Configure、Job 操作→Operate、Rollout 创建→ManageRollouts、审批→ApproveRollouts、节点维护→ManageNodes、Webhook 管理→ManageWebhooks）：无权限时禁用 + tooltip 说明所需权限
- [x] 3.2 身份显示：顶栏持久展示 principal id + 角色；principal 缺失（旧后端）显示「身份未知」并保持门控宽松（design D5 矩阵）
- [x] 3.3 403 执行期降级：变更失败为 403 时统一权限错误文案、不重试、并失效/重拉 system 查询
- [x] 3.4 前端测试：viewer 会话门控渲染、principal 缺失宽松退化、403 降级（`pnpm test`，`console/`）

## 4. 服务端分页/过滤

- [x] 4.1 Events 页：`event_type`/`stream_id`/`correlation_id`/`node_id` 结构化过滤 + 页码/页大小（25/50/100）控件，全部进 URL query，React Query key 含参数、`keepPreviousData`；移除任意文本 contains 过滤
- [x] 4.2 Audit 页：`resource_id` 过滤 + 分页控件进 URL；移除 `staleTime: Infinity`（分页导航需重新取数）
- [x] 4.3 operations 列表展示处（runtime 页 operation history 等）接 `operation`/`state`/`correlation_id`/`node_id` 过滤与分页
- [x] 4.4 前端测试：URL 状态往返（过滤/翻页/后退）、第 51 条可达性（mock 超过一页的数据翻到第 2 页）

## 5. 模式感知导航

- [x] 5.1 `hubMode` 门控导航：本地模式隐藏 Jobs/Rollouts/Approvals/Audit 导航项；capabilities 未决时保持完整导航（无启动闪变）
- [x] 5.2 被隐藏页 URL 直达渲染「仅 Hub 模式可用」解释态（非 404/非重定向）；Settings 页本地模式隐藏 Webhook 管理区
- [x] 5.3 前端测试：本地模式导航项缺席、URL 直达解释态、Settings 分区门控

## 6. 端到端验证

- [x] 6.1 `cargo test --workspace --all-targets` + `cargo clippy --workspace --all-targets` 全绿
- [x] 6.2 `console/` 全量 `pnpm test` + `pnpm build` 绿
- [x] 6.3 浏览器实跑（真实双模式）：Hub 模式验证 viewer 凭证门控 + 事件超 50 条翻页/过滤 URL 往返；本地模式验证导航隐藏 + 直达解释态 + 身份显示

## 7. 文档

- [x] 7.1 `docs/docs/` 控制台/控制面相关页面补：`/system` principal 回显契约、Console 角色感知与分页行为、本地模式导航语义
- [x] 7.2 zh-Hans 对应树同步翻译两页内容
- [x] 7.3 `pnpm docs:check` 绿（yaml 代码块分类标记、锚点、组件清单不受影响）
