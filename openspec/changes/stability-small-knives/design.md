# Design: Stability Small Knives

## Context

CODE-REVIEW-2026-11-15.md 批次 2~5 中 9 个低风险 P1。2026-11-16 存活盘点确认:其中 6 项为 PARTIAL(仓库里已有正确参照模式,#1365/#1366/#1367 修掉了同型问题的另一半)。参照模式:读路径 `redact_record_fields`(api/jobs.rs:87)、`PageQuery`/`page_items` 分页助手(api/mod.rs:372-426)、start 路径有界 teardown(kernel.rs:336-339 + `KERNEL_TEARDOWN_JOIN_TIMEOUT`:1544)、流校验临时资源(configuration.rs:341-357)、SQLite lease CAS、限流 `problem()` 响应(api/rate_limit.rs:340)。

## Goals / Non-Goals

见 proposal Non-goals。本设计补充实现级排除:不改 `Wal` 其余同步策略语义、不重排 kernel 生命周期结构、不引入新的 HTTP 中间件框架(只用 tower-http/tower 既有件)。

## Decisions

### D1: mutation 响应脱敏 = 机械包裹,不新建抽象

所有返回 JobRecord 的 mutation 端点(create/update/upgrade/rollback/restore/recovery/trigger/desired_state/action pause-resume 等)在 `Json(...)` 前对记录值调 `redact_record_fields`,与读路径同函数。不改函数签名、不搞响应包装 trait。理由:审查点就是"读路径有、写路径没有",最小修复是补齐对称性;抽共享 helper 反而扩大 diff。

### D2: 分页 = 复用 `Page<T>` 信封 + console 同步适配(BREAKING)

- 三个集合(hub jobs、rollouts、config approvals)接受 `page`/`page_size`,经 `page_items` 切片返回 `Page<T>`,与 /nodes、/streams 完全同型。
- 响应从裸数组改为 `{items, total, page, page_size}` 信封。**备选**:无参时返回裸数组做兼容——否决,双形状是长期负债,console 在库内同 change 适配(api.ts:752 `jobs()` 改读 `.items`;queries.ts:139 随之;rollouts/approvals 调用点同理),外部消费者以 BREAKING 记入 proposal。
- 分页为内存切片(hub.jobs() 已全量返回),与现有 `page_items` 行为一致;本次不把分页下推到 storage(那是另一个量级的重构)。

### D3: 每请求超时 = tower `TimeoutLayer` + `HandleErrorLayer` 映射为标准 problem 响应

tower-http 的 `TimeoutLayer` 固定返回裸 408,违反 control-plane-api "Standard API errors" 的信封约定。采用 tower 组合:`HandleErrorLayer::new(...)` + `TimeoutLayer`,把 Elapsed 映射为 `problem(408, "request_timeout")`,与限流 429 (`rate_limited`) 同构。超时时长取固定默认(如 30s)并进 `RateLimitConfig` 同级的控制面配置?——不:本批不做新配置面,先定常量并留 TODO 注释,配置化留给后续。两处栈(local :495-506、hub :866-868)都加。

### D4: PG lease 续约 = 单条 `UPDATE ... RETURNING`

postgres_methods.rs:3153-3162 的 guarded UPDATE + 独立 SELECT 合并为 `UPDATE cp_hub_lease SET ... WHERE id = 1 AND holder = ? AND expires_at_ms > ? RETURNING epoch`,rows_affected==0 → Lost 分支不变。SQLite 版(sqlite.rs:2115)保持现状:同连接内 UPDATE→SELECT 被连接互斥与写事务串行化,无此窗口;不为了对称性重写已正确的代码。契约测试(两后端)补并发续约场景。

### D5: WAL seq 归还 = 失败自由列表,不是 `fetch_sub`

共享单调计数器上的 `fetch_sub` 在并发预留下会双重分配 seq(T1 得 N、T2 得 N+1、T1 失败 fetch_sub → 下一个预留者又得 N+1)。正确做法:`Wal` 持 `Mutex<Vec<u64>>` 归还自由列表,`append` 先 pop 自由列表、空则 `fetch_add`;store 失败时把 seq 压回自由列表。store 层按 seq 键插入、不强制连续(store.rs:382-404),乱序到达无害;ack frontier 等最高连续序列,归还让空洞被下一次追加填上。失败是稀有路径,列表天然有界(≤ 并发预留数)。GroupCommit/Periodic 分支不动(其预留已在 staging 锁内,注释自述无此问题)。

### D6: stop/stop_all 有界落定 = 复用 teardown 常量,超时后 detach

stop/stop_all(kernel.rs:1094,1118)的裸 join 改为 `timeout(KERNEL_TEARDOWN_JOIN_TIMEOUT, &mut handle)`;超时则 `warn!` 后继续 detach——与 start 路径(await_previous_teardown 文档注释)语义一致:楔死的内核自己收敛,启停路径不永久持 `_start_guard`。detached 后仍执行 `state.close()` 与 session 移除(与现状一致)。不把 await_previous_teardown 改成公共助手再复用(其返回值语义是 start 专用),stop 处直接用 `tokio::time::timeout` 三行实现,注释互相引用。

### D7: Kafka close flush = `spawn_blocking`

kafka.rs:393 的 `producer.flush(30s)` 移入 `tokio::task::spawn_blocking`(producer 为 `Arc`,clone 进闭包)。close() 保持 async,`.await` 结果。build() 的后台 flusher 已在 #1366 接 cancellation_token,本批不再动。

### D8: nodes 注册锁收窄 = label 读移出写锁

hub/nodes.rs:499-509:`get_node_labels().await` 移到 `nodes.write()` 之前。放松的一致性:注册与标签快照不再原子——标签本来就是独立可变资源(有专门的 label 管理 API),注册后 label 更新走既有路径,无不变量依赖此原子性。测试钉扎:慢 label 存储下并发 register/report 不被阻塞(用可注入延迟的 fake storage)。

### D9: Job 校验临时后端 = 与流路径同型的 with_temporary

job_runner_adapter.rs `validate_local_job`(:31)经 `local_state_backend`(:357)打开真实 durable root 的 redb。改为:校验路径对 `state.root` 使用临时目录(tempfile)构造 `RedbStateBackend`,校验完 drop(锁随 drop 释放)——与流校验 `StreamJobAdapter::with_temporary`(configuration.rs:341-357)同构。实现为 `local_state_backend(plan, root_override: Option<&Path>)` 或独立 `validation_state_backend`,取改动更小者;`max_bytes`/`format_version` 校验语义保留。

## Risks / Trade-offs

- [分页信封破坏外部消费者] → BREAKING 标记 + console 同 change 适配 + docs 两语言树同步;release notes 说明。
- [mutation 脱敏误伤非凭据字段] → redact_record_fields 已在生产读路径使用,字段集合已被验证;不新增字段规则。
- [408 vs 504 语义争议] → 408 与 tower-http TimeoutLayer 缺省一致,且 body 现在是标准信封;文档写明。
- [stop 超时 detach 后 state.close() 仍可能阻塞] → 保持与现状一致(state.close 是同步短路径),本批只解决无界 join;若 close 也楔死属另一问题,报告留档。
- [label 读出锁后注册瞬间标签可见性] → 标签为最终一致资源,无 spec 依赖原子注册;测试覆盖并发不阻塞即可。
- [WAL 自由列表引入新状态] → 仅失败路径写入、append 开头一次 pop,成功路径零开销;单测覆盖"失败后 seq 重用且无重复"。

## Migration Plan

单 PR 合入,无持久数据迁移。分页信封对 console 的适配在同一 PR 内,合入即一致;外部 API 消费者按 release notes 迁移。回滚 = revert PR,无状态残留。

## Open Questions

(无——上述 D1-D9 均已定案;超时配置化留 TODO 属实现注释,不阻塞。)
