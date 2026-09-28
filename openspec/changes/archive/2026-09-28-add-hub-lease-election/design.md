# Design: Hub 租约选主（hub-ha 阶段 2）

## Context

阶段 1 已落地双存储后端（`StorageBackend` trait + SQLite/PostgreSQL + `StorageActor` FIFO actor，`storage/mod.rs:1712`、`mod.rs:592`）。Hub 启动序为"恢复持久态 → 绑监听 → 无条件启动 sweep/reconcile/maintenance 周期任务"（`lib.rs:486-584`），全部状态为单进程内存。阶段 2 的目标：多 Hub 实例共享同一持久库，任一时刻至多一个实例作为 leader 提供控制面服务，leader 死亡后 standby 在有界时间内接管。`hub-ha/architecture.md` 推荐 advisory lock，本设计改用租约行 CAS（见 D1）。

## Goals / Non-Goals

**Goals**

- 持久库租约（CAS + fencing epoch）选出唯一 leader；多实例共存安全。
- standby 行为明确：不调和、不接受读写（健康/就绪/metrics 除外）、readiness 如实报告。
- 晋升前重新从持久库恢复完整控制面视图；让位即停周期任务。
- 默认关闭（`ha.enabled=false`）时行为与现状逐位一致。
- 双后端实现同一租约契约；契约测试可离线（SQLite）运行，PG 测试沿用 `ARKFLOW_TEST_POSTGRES_URL` 门控。

**Non-Goals**（见 proposal）

## Decisions

### D1：租约行 CAS，而非 PG advisory lock

`cp_hub_lease` 单行表（`id=1` 恒定）：`holder TEXT NOT NULL`、`epoch BIGINT NOT NULL`、`expires_at_ms BIGINT NOT NULL`、`updated_at_ms BIGINT NOT NULL`。

- `try_acquire(holder, ttl_ms, now)`：先 `INSERT .. ON CONFLICT DO NOTHING`（幂等建行，epoch=0、expires_at=0），再 `UPDATE SET holder=$h, epoch=epoch+1, expires_at_ms=$now+$ttl, updated_at_ms=$now WHERE id=1 AND (expires_at_ms <= $now OR holder=$h)`。影响行数 1 → `Acquired { epoch }`（holder 已是自己时视为无 epoch 跳跃的续约）；否则返回 `HeldByOther { holder, epoch, expires_at_ms }`。
- `renew(holder, ttl_ms, now)`：`UPDATE SET expires_at_ms=$now+$ttl, updated_at_ms=$now WHERE id=1 AND holder=$h AND expires_at_ms > $now`。影响行数 1 → `Renewed { epoch }`，否则 `Lost`。
- `release(holder, now)`：`UPDATE SET expires_at_ms=$now, updated_at_ms=$now WHERE id=1 AND holder=$h`（立即过期，epoch 不变，下一次 acquire 递增）。优雅关停用。

**为什么不用 advisory lock**：advisory lock 绑定单条 PG 会话，而现有 `StorageActor` 是 pool + FIFO actor 模型，持锁需要绕过 actor 长期占用一条连接，破坏既有抽象且无法在 SQLite 上测试同一契约。租约行经 actor 走、天然给出单调 fencing epoch、两后端可参数化测试。对 2-3 实例规模（architecture.md 的首选部署），CAS 竞争开销可忽略。

**时钟**：`now` 由 Hub 侧传入（与节点租约 `lease_ttl_ms`、`mark_stale` 的既有墙钟实践一致）。部署假设 NTP，TTL（默认 15s）远大于合理偏移。文档明示。

### D2：leadership 状态机与门控方式

`Hub` 增加 `leadership: Arc<RwLock<Leadership>>`：

```rust
enum Leadership {
    /// ha 未启用：等同现状（永远允许）。
    Disabled,
    Standby { since_ms: u64 },
    Leader { epoch: u64, since_ms: u64 },
}
```

- 周期任务（sweep/reconcile/maintenance）**不按领导态增删 spawn**，改为每个 tick 先查 `is_leader().await`，standby 时跳过该 tick。实现最小、无任务生命周期竞态；standby 时空转成本是每 500ms/1s/60s 一次原子读。
- HTTP 门控用 axum middleware（`hub_router` 上 `layer(middleware::from_fn_with_state(...))`）：路径在允许清单（`health_path`、`readiness_path`、`liveness_path`、`/metrics`）内的放行，其余在非 Leader（含 Disabled 之外的 standby）时返回 `503` problem JSON（`code: "hub_standby"`，带 `Retry-After: 2`）。standby 的内存视图是空的/陈旧的，提供读会误导操作员与 LB 探测——统一拒绝，LB 依据 readiness 摘除。
- `is_leader()` 对 `Disabled` 恒真，保证默认关闭时中间件零生效（仍走原路径）。

### D3：选主循环与晋升/让位流程

`serve_hub` 中当 `ha.enabled` 时额外 spawn 一个 lease 任务，周期 `ttl/3`（下限 1s）：

- **standby → leader（promote）**：`try_acquire` 成功后：
  1. 清空并重载持久化视图：`recover_persisted_state()`（jobs/versions/checkpoints/operations/rollouts，需按"先 clear 后 load"实现，覆盖 ex-leader 残留的陈旧内存条目）+ `restore_persisted_operations()`；节点注册表与 `placement_order` 同步清空（节点租约本就短暂，Agent 会向新 leader 重新注册）。
  2. 全部恢复成功后才把 leadership 置为 `Leader { epoch }`；任一步失败 → `release` 租约、保持 standby、记录失败（fail-closed，不服务半恢复状态——对齐 `secure-durable-control-plane` 的 readiness 语义）。
- **leader 续约**：`renew` 成功继续；`Lost` 或存储错误 → **step down**：leadership 置 `Standby`，广播事件 + 日志 + metrics。周期任务在下个 tick 因门控自动停摆；无需强杀。
- 优雅关停（cancellation 触发）：leader 时尽力 `release`，让 standby 立即可接管而不是等 TTL。

### D4：readiness / 观测

- `hub_readiness` 输出增加 `ha` 字段：`{enabled, role: "leader"|"standby"|"disabled", epoch}`；standby 时 readiness 返回 503（现有 body 结构扩展，不破坏）。`Disabled` 时输出与现状兼容（新增字段仅增量）。
- metrics：`arkflow_hub_leadership_role{role}` gauge（当前角色为 1）、`arkflow_hub_leadership_epoch`、`arkflow_hub_leadership_transitions_total`；转换同时发 `HubEvent` + `tracing::warn/info`。

### D5：配置面

`HubConfig` 增加 `ha: HubHaConfig`（默认 `enabled: false`）：

```
HubHaConfig { enabled: bool, lease_ttl_ms: u64 (默认 15000), holder_id: Option<String> }
```

- bin 侧环境变量：`ARKFLOW_HUB_HA_ENABLED`、`ARKFLOW_HUB_HA_LEASE_TTL_MS`、`ARKFLOW_HUB_HA_HOLDER_ID`（缺省生成 `"{hostname}:{pid}:{boot_ms}"`）。
- 启动校验：`ha.enabled=true` 时必须有持久存储（复用 `validate_hub_startup` 的外部绑定校验路径，缺存储直接拒绝启动——standby 无库可谈）。

### D6：存储契约扩展

`StorageBackend` 新增 `try_acquire_hub_lease / renew_hub_lease / release_hub_lease` 三方法，`ControlPlaneStore` 双分派，`StorageActor` 包装三个公开方法（与其他方法同构）。SQLite DDL（`sqlite.rs`）与 PG DDL（`postgres.rs`）各自幂等建表；`migrate_tool` 的表清单不包含该表（运行时状态，不迁移）。

## Risks / Trade-offs

- [时钟偏移导致双主] → TTL 默认 15s 远大于 NTP 管控下的偏移；文档要求 NTP；epoch 单调供后续阶段做写围栏。
- [让位瞬间的在途写（TOCTOU）] → 写路径在 handler 入口查 leadership，检查后到写入完成的窗口内可能丢失租约；该窗口有界于续约周期，且下一个周期任务 tick 即停。全量存储写围栏（每写带 epoch 校验）为阶段 3 范围，本设计不声称消除该窗口。
- [standby 空转请求] → 每 tick 一次租约 CAS 与原子读，2-3 实例下可忽略。
- [SQLite 多进程争锁] → HA on SQLite 明确不支持（文档 + 启动 warn），契约实现仅为测试服务。
- [promote 后节点注册表为空] → 与 Hub 冷重启现状一致（Agent 重连即重注册），无新语义。

## Migration Plan

- 仅新增 `cp_hub_lease` 表（两后端幂等 DDL），无数据迁移。
- 部署：升级二进制 → 配 `ARKFLOW_HUB_HA_ENABLED=true` + 共享 PG → 先起一个（立即成为 leader）→ 再起 standby。
- 回滚：`ARKFLOW_HUB_HA_ENABLED` 去掉即回到单实例行为；表留存无害。

## Open Questions

（无——阶段边界已由 architecture.md 与 proposal Non-goals 界定）
