## 1. 存储层

- [x] 1.1 `StorageBackend` 新增 `list_webhooks` / `replace_webhooks` 方法与 `WebhookSubscription` DTO（id/url/event_types/enabled/secret/created_by/created_at_ms），入 `storage_ops!` 宏表（仿 node_labels 行 `storage/mod.rs:860-867`），ControlPlaneStore 委托与双方言分发补齐
- [x] 1.2 SQLite 建表 `cp_webhooks`（`CREATE TABLE IF NOT EXISTS`，与 `sqlite.rs:2444+` 同式）+ 两方法实现；契约测试：持久化跨 store 重开、全量替换语义、secret 不出现在任何非 secret 列
- [x] 1.3 Postgres 方法实现（`postgres_methods.rs`，占位符方言惯例）；`ARKFLOW_TEST_POSTGRES_URL` 门控契约测试镜像 SQLite 断言

## 2. 投递内核

- [x] 2.1 `hub/webhooks.rs` 转发 worker：订阅 `updates` broadcast，按 event_type 前缀过滤，每订阅串行投递任务 + 容量 64 有界队列（满则丢最旧 + dropped 计数），`Lagged` 记 warn 继续
- [x] 2.2 投递执行器：reqwest POST（10s 超时）、body 为 HubEvent wire JSON、`X-Arkflow-Event-Id/Type/Timestamp` 头、有 secret 时 HMAC-SHA256 `X-Arkflow-Signature`（hex of body）；2xx 成功，否则 1s/4s/16s 退避共 3 次尝试
- [x] 2.3 自动暂停：连续 5 次投递失败 → 持久化 `enabled=false` + 发布 `webhook.paused` 事件（含订阅标识）；订阅集变更时 worker 热更新订阅列表
- [x] 2.4 leader-only：worker 感知租约（仿 `hub/leadership.rs`），非 leader 停止消费与投递、接管后从新事件续（不重放）
- [x] 2.5 单元测试：签名可验证、过滤不匹配不投、重试成功计数重置、持续失败暂停、队满丢最旧不阻塞、暂停后不投

## 3. API 与授权

- [x] 3.1 `api_contract.rs`：`OperatorAction::ManageWebhooks`（Admin/Operator 持有、Viewer 不持有）+ `WebhookSetRequest`/`WebhookResponse`（不含 secret）/test 请求响应 DTO；角色矩阵单测
- [x] 3.2 `api/webhooks.rs` 路由：`GET /webhooks`、`PUT /webhooks`（全量替换，校验 ≤16/URL scheme/event_types 非空项；某订阅 enabled 由 false→true 迁移时发 `webhook.resumed` 事件）、`POST /webhooks/{id}/test`；`require_operator_action(ManageWebhooks)`；全部成功/拒绝入 `record_audit`（不含 secret 值）；挂载进 router（仿 `api/mod.rs:698` 路由表）
- [x] 3.3 测试投递端点：同步投递一条合成 `webhook.test` 事件，返回 HTTP 状态或传输错误；不持久化事件、不影响失败计数与 enabled
- [x] 3.4 API 测试（仿 `node_labels_api_tests.rs`）：401 矩阵覆盖新路由、viewer 403+审计、替换校验拒绝（17 条/ftp://）、secret 不回显、test 端点行为

## 4. Console

- [x] 4.1 `api.ts` 客户端方法（list/replace/test）+ `types.ts` 类型；`settings.tsx` 空壳填入 Webhook 管理面：列表、创建/编辑（URL + event_types + secret）、启停、删除、测试按钮（显示结果）
- [x] 4.2 i18n en/zh 全 key（zh 不留 Partial 缺口）；`pnpm lint` + `pnpm build` 绿

## 5. 文档与门禁

- [x] 5.1 `docs/docs/` 新增（或并入控制面运营页）webhook 告警章节：**投递语义一节（best-effort 总述 + 四层丢失源：lease 交接/broadcast lag/队满丢最旧/暂停期间）**、「第二个订阅过滤 `webhook.` 前缀做死信监控」推荐模式、订阅管理、签名验证示例（接收方校验 HMAC 的 curl/代码样例，yaml 块带 validate 标注）、暂停/恢复语义、SSRF 责任边界说明；`docs/sidebars.ts` 如需登记
- [x] 5.2 zh-Hans 对应树同步翻译页
- [x] 5.3 `docs/docs/reference/api.md` 增补三个路由条目（en/zh）
- [x] 5.4 全量门禁：`cargo test --workspace --all-targets`、`cargo clippy --workspace --all-targets`、`cargo fmt --check`、`pnpm docs:check`
