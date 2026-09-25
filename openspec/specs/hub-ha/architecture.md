# Capability: Hub 高可用架构前置调研

## 概述

Hub 当前为单实例部署（SQLite 单写者、无故障转移）。本文档调研高可用架构选项，为后续实现提供决策基础。

## 1. Leader Election 机制对比

| 方案 | 优点 | 缺点 | 适用场景 |
|------|------|------|---------|
| Raft 共识 | 强一致性、自动选主、无需外部依赖 | 实现复杂、增加 3+ 节点延迟 | 大规模多 Hub 集群 |
| 外部协调服务 (etcd/ZooKeeper) | 成熟方案、自动故障检测 | 引入外部依赖、运维成本 | 已有 etcd/ZK 基础设施 |
| DB 租约 (PostgreSQL advisory lock) | 利用现有 DB、实现简单 | 依赖 DB 可用性 | 已有 PostgreSQL 部署 |
| SQLite 文件锁 | 零依赖 | 仅单机、不支持跨节点 | 不适用 |

**推荐**：DB 租约（PostgreSQL advisory lock）——利用现有 sqlx PG 支持，实现最简，满足 2-3 Hub 实例的选主需求。

## 2. 存储后端选项

| 后端 | 一致性 | 可用性 | 分区容忍 | 适用场景 |
|------|--------|--------|---------|---------|
| SQLite (当前) | 单写者 | 单点 | 不适用 | 开发/单节点 |
| PostgreSQL | 强一致 (WAL) | 高 (流复制) | 网络分区时降级 | 生产推荐 |
| etcd | Raft 强一致 | 高 (3+ 节点) | 网络分区时可能不可用 | 已有 etcd 基础设施 |

**推荐**：PostgreSQL——已有 sqlx 集成经验，支持流复制实现读扩展。

## 3. Agent 重连策略

Hub 故障时 Agent 行为（`agent.rs` 重连循环）：

1. 心跳/轮询失败 → 指数退避重连（`equal-jitter`）
2. 重连成功 → 重新注册（`boot_id` 相同则恢复会话）
3. 数据面不受影响——Agent 本地 Stream/Job 继续运行
4. 配置/Job 变更在 Hub 恢复后由 Agent 拉取（reconciliation）

**改进方向**：
- Agent 缓存最近已知 Hub 地址列表，支持多 Hub 故障转移
- Agent 数据面增加本地 WAL 持久化（已有），确保 Hub 故障期间不丢数据

## 4. 数据面保证

Hub 故障时数据面行为：
- 已运行 Stream/Job 继续运行（Agent 独立管理数据面）
- 新 Job/配置变更暂不可用
- checkpoint 照常执行（Agent 本地触发）
- 恢复后 Agent 重新注册，Hub 通过 reconciliation 恢复管理

## 5. 迁移路径

### 阶段 1：单 Hub + PostgreSQL
- 存储从 SQLite 迁移到 PostgreSQL
- Hub 仍为单实例，但故障后可在另一节点快速恢复

### 阶段 2：Leader Election
- 引入 DB 租约选主
- 多 Hub 实例共享 PostgreSQL
- 非 leader Hub 进入 standby 模式

### 阶段 3：完整 HA
- 自动故障检测与选主
- Agent 自动发现新 leader
- 零数据丢失保证（同步复制）

## 6. 决策记录

| 决策 | 结论 | 理由 |
|------|------|------|
| Leader Election 机制 | DB 租约 | 利用现有 PG 集成，实现最简 |
| 存储后端 | PostgreSQL | 已有 sqlx 经验，支持流复制 |
| Agent 重连 | 指数退避（已有） | 无需修改 |
| 首选部署模式 | 2 Hub + 共享 PG | 满足可用性要求，成本可控 |

## 7. 参考架构

```
                    ┌─────────────┐
                    │  LB / VIP   │
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
       ┌──────┴──────┐    │     ┌──────┴──────┐
       │   Hub #1    │    │     │   Hub #2    │
       │  (leader)   │    │     │ (standby)   │
       └──────┬──────┘    │     └──────┬──────┘
              │           │            │
              └─────┬─────┴────────────┘
                    │
              ┌─────┴─────┐
              │PostgreSQL │
              │ (primary) │
              └───────────┘
```
