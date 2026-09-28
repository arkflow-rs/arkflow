## Why

Hub 是控制面单点：全部舰队状态活在单进程内存（`crates/arkflow-server/src/hub.rs:123-147` 的 `RwLock<BTreeMap<..>>` 集合），命令队列 memory-only，唯一恢复手段是"对同一持久库冷重启另一个进程"（`lib.rs:486-521` 的 `serve_hub` 启动序：恢复 → 绑监听 → 周期调和）。Agent 只配单个 `hub_url`（`agent.rs:41`），Hub 进程死亡期间整个控制面（作业提交、调和、rollout、checkpoint 调度）不可用。`openspec/specs/hub-ha/architecture.md:55-58` 已把"DB 租约选主、多 Hub 实例共享 PostgreSQL、非 leader 进 standby"定为阶段 2；阶段 1（PostgreSQL 存储后端）已于 2026-09-25 落地，选主的前置条件已就绪。

## What Changes

- 在持久库中新增控制面租约（单行 CAS：holder、fencing epoch、expires_at），`StorageBackend` 增加 acquire / renew / release 三个方法，SQLite 与 PostgreSQL 双后端实现同一契约（SQLite 供开发与测试，生产 HA 要求 PostgreSQL）。
- Hub 引入 leadership 状态机：启动时按配置决定是否参与选主；standby 周期性尝试 acquire，leader 周期性（ttl/3）renew；renew 失败或丢失即让位（step down）为 standby。
- standby 模式行为：不跑 sweep / reconcile / maintenance 周期任务；除 `/health`、`/liveness`、`/readiness` 外的所有路由（operator 与 agent 面）返回 503；readiness 报告未就绪并标注角色。
- 晋升为 leader 时先重新从持久库恢复（jobs / versions / checkpoints / operations），完成后再开放读写与调和——保证接管的是前任 leader 的最新持久状态。
- 让位时立即停止周期任务、关闭 readiness，运行中的写请求以其完成为界（僵尸窗口有界于 lease TTL）。
- 配置：`ha.enabled`（默认 false，单实例行为逐位不变）、`ha.lease_ttl_ms`、`ha.holder_id`。默认关闭时零新行为、零新表写入。
- 可观测：leadership gauge（role）、epoch、最近一次转换时间与原因进入 metrics 与 events 流。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `hub-ha`: 新增"控制面租约选主"需求——租约 CAS 语义（acquire/renew/release + fencing epoch）、leader/standby 行为边界、晋升前重恢复、让位语义；存储后端契约扩展到租约方法（双后端一致）。
- `secure-durable-control-plane`: readiness 语义扩展——启用 HA 时 readiness 除"持久恢复完成"外还要求"持有租约的 leader"；standby 不提供部分内存状态的读写。

## Impact

- `crates/arkflow-server/src/storage/`：`StorageBackend` trait 新增租约三方法 + DDL（`cp_hub_lease` 表）+ SQLite/PostgreSQL 实现 + 契约测试。
- `crates/arkflow-server/src/hub.rs` 与 `hub/` 子模块：leadership 状态、晋升/让位流程、mutating 路由的 leader 门控。
- `crates/arkflow-server/src/lib.rs`：`serve_hub` 启动序改造（选主任务、周期任务按 leadership 门控）、路由层 standby 503、readiness 输出角色。
- `crates/arkflow-server/src/bin/arkflow-server.rs` 与 `ServerConfig`：HA 配置面。
- 文档：`docs/docs/` 与 zh-Hans 对应页（部署/HA）。
- 无数据模型破坏性变更；默认关闭时对现有部署零影响。

## Non-goals

- 不做阶段 3 的 Agent 多 Hub 地址发现 / 自动 leader 路由（Agent 侧仍单 `hub_url`，靠 LB/VIP 前置）。
- 不做存储同步复制、读副本、流复制读扩展。
- 不在 SQLite 后端上承诺多实例 HA（契约一致仅用于测试；生产 HA 文档明确要求 PostgreSQL）。
- 不实现每条存储写入都携带 fencing epoch 的存储级围栏（本阶段以"让位即停写 + TTL 有界僵尸窗口"为正确性边界；全量写围栏留给阶段 3）。
- 不改动数据面与 Agent 执行路径。
