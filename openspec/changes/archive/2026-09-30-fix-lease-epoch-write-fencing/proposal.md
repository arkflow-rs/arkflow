## Why

P1-2（`openspec/CODE_REVIEW_2026-09-29.md`，控制面深潜核实）：Hub 租约选主的 **fencing epoch 是纸面围栏**。租约 CAS 本身正确（`sqlite.rs` 的 takeover 递增 epoch、`renew` 条件 UPDATE、Postgres 镜像），`leadership.rs` 的注释也宣称 "Fencing epochs keep a recovered store consistent"——但 epoch 只出现在租约类型与 `/system`、`/readiness` 的展示层，`StorageBackend` 的全部写方法不携带 epoch，存储层没有任何拒绝旧写的谓词。

后果：leader 因 GC 停顿/网络分区超过 TTL、standby takeover（epoch+1）后，旧 leader 在其下一个续期 tick（≤ ttl/3）感知 Lost 之前，reconcile 循环仍在并发写 desired state、upsert operation、派发命令——**有界的双 leader 写窗口真实存在**（内存态 step_down 不回滚同一 tick 内已进入的写）。这正是 `hub-ha` spec 增量合并时承诺的 fencing 语义。

## What Changes

- **写路径围栏**：`StorageCommand` 新增 `Fenced { claimed_epoch, Box<command> }` 信封变体——actor 在**执行期**对照租约表的当前 epoch 校验声明值，失配以新错误 `StorageError::StaleLeader` 显式拒绝（不执行）。61 个 handle 方法中 **38 个变更类**经 `send_fenced` 发送（发送时捕获领导者 epoch），20 个读与 3 个租约操作直通。
- **epoch 接线**：`StorageActor` 句柄携带 `Arc<AtomicU64>`（当前领导 epoch；acquire/renew 成功写入、step-down/失去写 0）；`serve_hub` 的选举 tick（`run_election_tick`）更新它。
- **后端谓词**：`StorageBackend` 新增 `current_lease_epoch() -> Option<u64>`（读单例租约行），SQLite/Postgres 双实现 + 契约覆盖。
- **错误映射**：`StaleLeader` → HTTP 503 `stale_leader`（`hub_problem`），reconcile 侧日志可见。
- **契约测试**：takeover 后旧 epoch 写被拒、新 epoch 写通过、无租约（standalone）直通、租约三操作豁免、promote 后（epoch 已更新）recovery 写通过。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `hub-ha`: "控制面租约 SHALL 以持久库 CAS 语义选出唯一 leader" 需求增补写路径围栏条款与场景（stale 写被拒 / standalone 直通 / 租约操作豁免）。

## Impact

- `crates/arkflow-server/src/storage/mod.rs`（Fenced 变体、send_fenced、nack、actor 校验、StaleLeader、current_lease_epoch 分派、38 个 wrapper 改造、契约测试）
- `crates/arkflow-server/src/storage/{sqlite,postgres,postgres_methods}.rs`（`current_lease_epoch` 实现）
- `crates/arkflow-server/src/hub/leadership.rs`（选举 tick 同步句柄 epoch）
- `crates/arkflow-server/src/lib.rs`（`hub_problem` 映射 StaleLeader→503）
- 无配置面变更、无 wire 变更、无破坏性变更；未启用 HA 的部署（无租约行）行为逐位不变。

## Non-goals

- 不实现阶段 3（Agent 多 Hub 发现、存储级写围栏的分布式令牌传播）；本变更的围栏是 Hub 进程内声明 + 存储行校验，防的是"旧 leader 进程仍在写"，不是跨进程伪造 epoch（cluster 内均为持 token 的 Hub）。
- 不做 StaleLeader 的即时响应式 step-down（仍由下一个续期 tick 收敛，≤ ttl/3；围栏本身已封死正确性）。
- 不改 45 方法签名的公共 API（信封在 actor 内部展开）。
- 不动 standby 中间件、增量 failover 的其余语义。
