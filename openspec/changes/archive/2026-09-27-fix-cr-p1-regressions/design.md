## Context

#1247 引入了秘密引用、OIDC 联邦、分布式回执与 AI 后端等能力，同时改动了两处关停语义与一处 Hub 派发路径。CR 确认这些改动各自"半落地"：文档与测试描述的行为（drain window、迟到回执窗口、失败状态机接线）在代码中并未实现。本变更以最小手术恢复既定语义，不引入新机制。

关键现状（file:line 以 HEAD 为准）：

- `wal/mod.rs:562-570`：parked-ack 的 `select!` 只剩 `notified.await`；`stream_adapter.rs:811-919` 的测试已按"15s drain window"编写并通过（因为发布发生在窗口内），但窗口本身不存在。
- `remote.rs:2494-2497`：`pending.abort_all()` 无条件执行；提交前代码为 `if result.is_err() { pending.abort_all(); }`。`pump_cancel_tests::shutdown_aborts_unreceipted_branches_after_flushing`（remote.rs:3657）直接驱动 `pump_edge` 且无读循环，依赖 shutdown 路径的 abort。
- `hub.rs:2514-2527`：预解析失败返回 `Err` 后 outbox 租约（storage.rs:2060，30s）到期重领。storage 的 `CompleteAttempt` 处理器（storage.rs:2503-2507）已把未知 failure_class 映射为 intent `blocked`，rollout 协调器（hub.rs:4923-4937）把 `blocked` 映射为目标 `failed`——失败路径已存在，只是没接上。
- 五个 kind 模块的 `init()` 用 `OnceLock<()>` 吞掉 `register_components()` 的错误；`lib.rs:36-56` 的 `OnceLock<Result<(), String>>` 是仓库内既有范例。
- `server/lib.rs:3294`：`validate_configuration` 缺 `authorized()`（同路由 apply/draft 均有，见 3328/3249）。console 的共享 `request()`（console/src/api.ts:270-296）统一附带 Bearer token 并在 401 时重定向 OIDC 登录，加鉴权不影响前端。
- `core/config.rs:274-275`：`serde_json::from_value` 错误内嵌违规值；`configuration.rs:172-181` 已有值无关固定消息的先例。
- `output/pgvector.rs:141-158`：连接检查与查询分两次加锁，中间 `expect` 与 `close()` 竞态。

## Goals / Non-Goals

**Goals:**

- 恢复/落地七处 P1 修复，全部通过既有测试 + 新增回归测试验证。
- 每个修复保持最小 diff，不改变线格式、存储格式或公开 API。

**Non-Goals:**

- 不处理 P2/P3（OIDC PKCE/cookie、OTel filter、milvus 重试、向量工具去重等）。
- 不改变 at-least-once 语义或回执协议。
- 不重构 Hub 状态机。

## Decisions

**D1 — WAL drain window 用常量 + 原子毫秒字段（P1-1）**
`input-durability` 规格的「优雅关闭时的 parked 确认 drain」REQUIREMENT 已写明目标行为且**窗口为 30s**（规格为准；`stream_adapter.rs` 测试注释中的 "15s" 系笔误，一并修正）。实现：新增 `WAL_ACK_DRAIN_WINDOW: Duration = 30s` 常量与 `ack_drain_window_ms: AtomicU64` 字段（默认取常量）。parked 分支：`select! { notified, close.cancelled() }`；close 触发后再 `select! { notified, sleep(window) }`，窗口耗尽返回 `"WAL closed while acknowledgement was pending"`（规格原文）。测试通过 `#[cfg(test)] pub(crate)` 覆写方法缩短窗口，避免测试真等 30s。备选（测试真等 / `start_paused` 时间模拟）分别牺牲套件速度与多线程运行时兼容性，弃用。规格不变，实现合规。

**D2 — pump 仅在 Err/强制关闭路径 abort（P1-2）**
在 `pump_edge` 内用 `drained` 标志区分退出原因：通道关闭（`recv_async()` 返回 `Err`）置位。退出后 `if result.is_err() || !drained { pending.abort_all() }`。排空路径的最终清理由同连接的回执读循环拥有（remote.rs:1887 已有 `abort_all`），迟到回执经 `receipt_pending.apply` 正常 ack。这样 `shutdown_aborts_unreceipted_branches_after_flushing`（无读循环、shutdown 路径）与 `wire_write_failure_aborts_the_registered_branch`（Err 路径）语义不变。同步修正模块 doc（remote.rs:30-40）中对"每条退出路径都 abort"的错误描述。

**D3 — 预解析失败走 `complete_attempt` → intent blocked（P1-3）**
失败分支：`tracing::warn!` + `storage.complete_attempt(&attempt.attempt_id, "failed", Some("invalid_config"))` + `mark_outbox_processed` + `return Ok(None)`。`invalid_config` 不在可重试 class 集合（storage.rs:2479）中，落 `Some(_) if state != "succeeded"` 分支 → intent `blocked` → rollout 目标 `failed`。不派发命令给 agent，不残留 in-memory operation（`enqueue_attempt` 未调用）。备选（新增专用 failure_class 枚举）引入面更大，弃用。

**D4 — init() 错误存入 OnceLock（P1-4）**
五个 kind 模块改为 `OnceLock<Result<(), String>>`（与 `lib.rs:36-56` 的 `INITIALIZATION` 同构）：首次调用执行 `register_components()` 并存结果，后续调用克隆返回。`Error` 经 `to_string()` 存储、以 `Error::Config(String)` 重构，绕开 `Error` 非 Clone 的限制。

**D5 — validate 端点加鉴权，保留解析行为（P1-5）**
给 `validate_configuration` 加与 apply 相同的 `State(cp) + HeaderMap + authorized()` 守卫。**有意偏离** CR 建议的第二半（验证路径不解析 env/file）：apply 端点对已认证操作者同样解析 env/file，单操作者模型下认证即完全信任，仅加鉴权即为最小修复；console 前端无需改动。

**D6 — 解析错误值无关化（P1-6）**
`parse_engine_config` 中 `from_value` 失败映射为固定消息 `"Configuration error: validation failed after secret resolution"`（与 `configuration.rs:172-181` 同型）。位置信息本就不存在（值树无行列），不损失可用信息。

**D7 — pgvector write 单次持锁（P1-7）**
`write()` 先查 `connected`，再单次获取 pool 锁并以 `let Some(pool) = guard.as_ref() else { return Err(Connection) }` 替代 expect，锁跨查询持有。close 在查询期间阻塞等待锁，查询在存活 pool 上完成，无 panic 路径。并发写本就串行于该锁（现状 157 行重新加锁后持锁执行），吞吐不变。

## Risks / Trade-offs

- [D2 排空路径 pending 存活至读循环退出] 若对端既不回执也不关闭，pending 由 `read_idle_timeout` 兜底（读循环超时 → pending 非空 → 失败 → abort）→ 既有机制，无泄漏窗口。
- [D1 窗口耗尽返回错误] 调用方若不重试，条目停在 WAL，重启后回放 → 与提交前行为一致，at-least-once 保持。
- [D3 blocked 意图需要操作者干预] 秘密修复后需重新 apply 生成新 intent → 符合"永久性配置错误不自动重试"的语义。
- [D4 init 失败由被动变主动] 此前静默残缺的进程现在启动即报错 → 预期行为；`registry_consistency` 等测试不受影响（成功路径不变）。
- [D5 console 未带 token 时 validate 401] `request()` 已统一处理 401 → OIDC 跳转 → 与 apply 行为一致。

## Migration Plan

纯缺陷修复，无配置/数据迁移。回滚 = revert 单个提交。按 P1-1 → P1-2 → P1-3 → P1-4 → P1-5 → P1-6 → P1-7 顺序独立提交粒度实现，每步附回归测试。

## Open Questions

（无——所有决策均有仓库内先例或既有测试锚定。）
