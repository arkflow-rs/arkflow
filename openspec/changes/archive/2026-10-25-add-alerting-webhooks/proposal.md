## Why

控制面事件目前只有一条对内出口：认证 SSE 流（`/events/stream`，`crates/arkflow-server/src/api/mod.rs:698` → `hub_event_stream`，`api/events.rs:45`），事件由 Hub 各模块经 broadcast 通道发布（`hub.rs:145`，容量 256；发布点 `hub/leadership.rs:204`、`hub/nodes.rs:712`、`hub/job_orchestration.rs:916`）并持久化到 `cp_events` 表（`storage/sqlite.rs:2595`，`record_event`/`prune_events` 带保留期）。**没有任何向外转发能力**：grep webhook/notification/slack/email 在 server 与 console 均零命中（PLANNING.md 11.2 第 2 条，对应 5.7 阶段 4 整条未动）。运维方要接告警（节点掉线、rollout 失败、审批待决）只能自己轮询 SSE 或 REST——运营级故障通知链路缺失。SSE 基建（持久事件、有界载荷规范 `control-plane-events`）、reqwest 依赖（`crates/arkflow-server/Cargo.toml:22`）、审计与 RBAC 框架全部现成，最小切口即可补齐。

## What Changes

- 新增 **webhook 订阅** 持久实体：`{ id, url, event_type 过滤, enabled, secret, 创建者, 时间戳 }`，Hub 存储层（SQLite + Postgres）新增表与方法，Hub 重启后订阅保留。
- 新增 **转发 worker**：订阅既有事件 broadcast 通道，按订阅的 event_type 过滤后 POST JSON 到目标 URL（与 SSE 相同的有界 `HubEvent` 载荷 + 事件 ID/类型/时间戳/HMAC-SHA256 签名头）。
- 新增 **投递语义**：有界内存队列 + 有限重试（指数退避）；持续失败自动暂停订阅并发出事件（SSE/Console 可见）；不承诺跨 Hub 重启的持久 at-least-once。
- 新增 **管理 API**：订阅 CRUD + 测试投递端点，授权要求新动作 `ManageWebhooks`（Admin + Operator），全部变更入审计。
- 新增 **Console 设置页**的 Webhook 管理面（列表/创建/启停/删除/测试），当前 `settings.tsx` 为空壳。

## Capabilities

### New Capabilities

- `webhook-alerting`: Hub 事件向外部 webhook 的订阅管理、有界载荷投递、签名、重试与自动暂停语义。

### Modified Capabilities

- `control-plane-identity`: "Resource-scoped RBAC" 需求的动作面新增 `ManageWebhooks`（Admin 全量、Operator 持有、Viewer 不持有），与既有 `ManageNodes` 等动作同级。

## Non-goals

- 不做跨重启的持久投递队列（outbox 式 at-least-once 磁盘排程）；重启期间的事件不补投。
- 不做告警内容模板/分级/路由规则引擎（event_type 精确/前缀过滤即止）。
- 不做 Slack/钉钉/Email 等特定接收方适配——通用 JSON POST + 签名头，由接收方适配。
- 不做 Console 的 webhook 投递历史/统计页面（暂停事件经既有 Events 页可见）。
- 不改 SSE 事件流本身（`control-plane-events` 契约零变化）。

## Impact

- **存储**：`crates/arkflow-server/src/storage/`（新表 `cp_webhooks` ×2 方言 + trait 方法 + storage_ops 宏行，仿 `node_labels` 模式 `storage/mod.rs:860-867`）。
- **Hub**：`hub.rs`/`hub/` 新转发 worker（订阅 `updates` broadcast），停机/领导权变更处理仿 `leadership.rs`。
- **API**：`api/` 新 webhooks 路由 + `api_contract.rs` 动作枚举扩展；`ManageWebhooks` 进入角色矩阵。
- **审计**：订阅 CRUD 经 `record_audit`（`storage/mod.rs:1209`）。
- **Console**：`api.ts`、`settings.tsx`、i18n（en/zh）。
- **依赖**：零新增——reqwest 0.13 已在 `arkflow-server` 依赖中。
- **文档**：`docs/docs/` 与 zh-Hans 树新增/更新 API 与控制面页面，README 不涉及（非数据面组件）。
