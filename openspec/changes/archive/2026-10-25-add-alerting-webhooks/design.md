## Context

事件管线现状：Hub 各模块 `updates.send(HubEvent)`（broadcast 容量 256，`hub.rs:145/236`）+ `record_event` 持久化（`cp_events`，带 `prune_events` 保留期）；SSE 端点 `hub_event_stream`（`api/events.rs:45`）用 `event_matches`（`api/events.rs:121`）按 `EventQuery{event_type, stream_id, correlation_id, node_id}` 过滤。`HubEvent{event_id, node_id, #[flatten] ControlEvent}`（`hub/wire.rs:138`），`ControlEvent{occurred_at_ms, event_type, stream_id, outcome, message, operation_id, correlation_id, actor}`（`arkflow-core/src/control.rs:261`）。载荷有界规范由 `control-plane-events` spec 承载（排除凭证/原始配置/无界错误文本）。存储层新增实体的成熟模式：`storage_ops!` 宏行（`storage/mod.rs:860-867` 的 node_labels 三方法）+ sqlite/postgres 双方言 + `cp_*` 表。reqwest 0.13 已是 server 依赖。

## Goals / Non-Goals

**Goals:**
- 运维方可注册 webhook 订阅并按 `event_type` 前缀过滤，Hub 主动 POST 转发匹配事件。
- 投递有界、可观测：有限重试 + 失败自动暂停 + 暂停事件进事件流。
- 订阅管理与投递全部经 RBAC + 审计，与控制面既有纪律一致。

**Non-Goals:**
- 跨重启持久投递队列、告警模板/路由引擎、特定接收方适配（见 proposal Non-goals）。

## Decisions

**D1：转发 worker 订阅 broadcast，而非轮询 `cp_events`。**
与 SSE 同源（`updates.subscribe()`），实时、零新增持久化读路径；落后即丢（`RecvError::Lagged`）与 SSE 消费者语义一致——反正不承诺持久 at-least-once。轮询表方案被否：引入双源一致性问题且延迟高。Lagged 时记一条 warn 并继续（不重放，不暂停订阅）。

**D2：每订阅一条有界队列 + 串行投递，队满丢最旧。**
容量 64（与 backpressure 纪律同向：有界，不无限缓冲）。队满策略：**丢队头（最旧）事件并计 dropped 计数**。理由：webhook 是实时通知通道而非记录系统（持久记录在 `cp_events`/SSE replay），接收方长时间挂掉时保留最新事件比保留最早事件更贴告警本质——丢新会造成「起点快照后全盲」，丢旧只是有空洞的近实时流；两种策略下送达序列都是原序子序列，丢旧不引入乱序。每订阅一个 tokio 任务串行 POST：保序、天然限并发（订阅数上限 16 封顶总并发）。替代方案全局并发池被否——每订阅串行让「慢接收方只阻塞自己」。

**D3：投递 = POST JSON（HubEvent 原样 wire 形态）+ 签名头。**
头：`X-Arkflow-Event-Id`、`X-Arkflow-Event-Type`、`X-Arkflow-Timestamp`、`X-Arkflow-Signature`（HMAC-SHA256 over body，密钥为订阅 secret，未设 secret 则无签名头）。成功判定：HTTP 2xx。非 2xx/超时（10s）/连接错误 → 重试，指数退避 1s/4s/16s 共 3 次尝试后判失败；连续 5 次投递失败 → 自动暂停订阅（`enabled=false` + 持久化 + 发 `webhook.paused` 事件）；人工 re-enable 时对称发 `webhook.resumed` 事件——`webhook.*` 事件本身可被订阅（过滤 `webhook.` 前缀），meta 订阅方需要 resumed 区分「仍暂停」与「已恢复」。重启用同款 reqwest（禁重定向跟随外域？保持默认，接收方责任）。载荷复用 `ControlEvent` 有界形态，零新增脱敏面——与 `control-plane-events` 的 bounded-payload 要求同构，spec 里独立成文不引用 SSE 条款。

**D4：订阅实体与存储。**
表 `cp_webhooks{ id TEXT PK, url TEXT, event_types TEXT(json array, 前缀匹配), enabled INTEGER, secret TEXT, created_by TEXT, created_at_ms INTEGER }`，上限 16 条（与 fleet 上限 256 同类的硬上限纪律）。secret 存储：明文入库（与现状 Hub env secret 同级，不入 Non-goal 的 Secret Manager；spec 要求 API 响应永不回显 secret）。方法：`replace_webhooks`(全量替换，仿 labels 全替换语义)/`list_webhooks` 两方法即可——订阅量小，CRUD 收敛为「读全量 + 全量替换」最小面。测试投递不落库，直接同步 POST 一条合成事件。

**D5：授权用新动作 `ManageWebhooks`（Admin+Operator），不复用 `Configure`。**
动作即审计语义：webhook 含 secret 且是外发通道，值得独立动作名出现在审计与 role 矩阵里。复用 `Configure` 会让审计语义含混；仅 Admin 又把日常运营方挡在外面（Operator 可管节点却不能管告警订阅，不合理）。

**D6：Worker 生命周期挂 Hub，领导权感知。**
转发 worker 随 Hub 启动（`tokio::spawn`，模式同 `api/mod.rs:998` 的 interval worker）；Hub HA 下仅 leader 转发（worker 订阅 leadership 变更，非 leader 时排空队列并暂停消费，仿 `hub/leadership.rs` 的租约语义）——避免双 Hub 重复投递。

**D7：Console 落在 Settings 页。**
`settings.tsx` 现为空壳（PLANNING 11.3 第 6 条），webhook 管理面（列表/创建/启停/删除/测试按钮）填入其中，导航零改动。

## Risks / Trade-offs

- [复合丢失模型] → 端到端语义是 best-effort，四层独立丢失源（lease 交接窗口、broadcast lag、队满丢旧、暂停期间）叠加。spec 以一句权威定义总括（接收方不得依赖 webhook 为唯一记录），文档以「投递语义」章节列全四层——不藏在各需求字缝里。
- [自动暂停的通知悖论：订阅者挂掉时唯一通知出口恰是该订阅] → `webhook.paused`/`webhook.resumed` 本身是普通控制面事件，可被第二个指向不同端点的订阅按 `webhook.` 前缀捕获——死信监控闭环零新增机制，作为推荐模式写入文档；Console 设置页同见 enabled 状态。心跳订阅/失败即发事件被否：失败风暴自激放大（webhook 事件触发投递失败再触发事件），节流复杂度不值。
- [test 端点即时回显 = 内网扫描原语] → 威胁模型校准后增量≈0：Operator 既有能力集已包含更强面（rollout 重配全 fleet 配置、数据面 http output 插件本可向任意 URL 发数据并回显）。不收仅 Admin、不砍回显——防的是自己人、降的是常用功能；scheme 校验保留（防配置错误），受信边界写入文档。
- [接收方慢/挂导致事件丢失] → 有界队列丢最旧 + 有限重试是刻意取舍（见 D2 论证）；恢复靠人工 re-enable，re-enable 与暂停均有事件可观测。
- [SSRF：内网地址探测] → 最小缓解：仅允许 `https://` 与 `http://` scheme；深度防护（DNS 重绑定、回环段黑名单）留后续。订阅目标与 test 目标视为 Operator 级受信操作。
- [双 Hub 重复投递] → D6 leader-only 消费；租约抖动窗口内最坏漏投（可接受，语义本就是 best-effort）。
- [secret 明文入库] → 与既有 Hub env secret 同级；API 永不回显、审计不含值。Vault 集成是既有 P3 缺口，不在此扩。
- [投递放大：高事件量 × 16 订阅] → 每订阅串行 + 10s 超时 + 事件类型过滤前置（worker 内先匹配再入队），最坏有界。

## Migration Plan

纯增量：新表（`CREATE TABLE IF NOT EXISTS`，与 `sqlite.rs:2444+` 既有建表同式）+ 新 worker + 新路由；无既有数据迁移。回滚 = 删路由/worker，表留存无害。

## Open Questions

无——以上决策均可在评审时推翻，但相互独立，不阻塞立项。
