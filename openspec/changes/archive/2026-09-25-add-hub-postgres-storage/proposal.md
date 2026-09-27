# Proposal: add-hub-postgres-storage

## Why

Hub 控制面存储当前绑定 SQLite（单写者、无故障转移），是 HA 路线（`hub-ha`）的阶段 1 阻塞项。评审级设计已产出（`openspec/specs/hub-ha/postgres-storage-design.md`，2026-09-24 归档），本变更按该蓝图实施。PLANNING.md 第八节 P1 项。

## What Changes

1. 抽取 `StorageBackend` async trait（与 `StorageCommand` 一一对应，~52 方法，签名不变），现有 rusqlite 逻辑原样搬入 `SqliteBackend`（行为零变化）。
2. 新增 `PostgresBackend`（sqlx PgPool）：17 表幂等 DDL（BIGINT/IDENTITY/COLLATE "C"/partial unique index 直译）、`RETURNING` 替代 `last_insert_rowid`、23505 冲突映射为既有领域错误。
3. `ControlPlaneStore` 变为 scheme 分派枚举：`ARKFLOW_HUB_STORAGE` 以 `postgres://`/`postgresql://` 开头 → PG，否则视为 SQLite 路径（现状零破坏）；`StorageError` 增加 `Pool(#[from] sqlx::Error)`。
4. 存储契约测试参数化：SQLite 全量跑；PG 在 `ARKFLOW_TEST_POSTGRES_URL` 存在时跑同一套，否则 `#[ignore]`。
5. `arkflow-server migrate --from sqlite:<path> --to postgres:<url>` 一次性迁移子命令（外键序逐表拷贝、IDENTITY setval、行数对账）。
6. 部署文档更新（en/zh）。

## Capabilities

### New Capabilities

（无——`hub-ha` capability 已存在，本变更是其阶段 1 实施）

### Modified Capabilities

- `hub-ha`: 从「设计文档产出」推进为「阶段 1（PostgreSQL 存储后端）已实现」——新增 StorageBackend 双后端契约、scheme 分派、迁移工具的行为需求。

## Impact

- `crates/arkflow-server/src/storage.rs`（5,204 行）拆分为 `storage/` 模块目录：`mod.rs`（trait/actor/枚举/分派）、`sqlite.rs`（现有逻辑搬移）、`postgres.rs`（新实现）、`migrate_tool.rs`（新）。
- `crates/arkflow-server/Cargo.toml`：新增 `sqlx`（workspace 依赖，仅 postgres + runtime-tokio 特性）。
- `crates/arkflow-server/src/bin/arkflow-server.rs`：`open` 调用点适配 + `migrate` 子命令入口。
- 测试：28 个存储单测转 async 契约套件参数化；hub/lib 内联测试的 `with_connection` 播种保持 SQLite 路径不变。
- 文档：`docs/docs/control-plane/deploy.md`（en/zh）PG 配置与迁移说明。

## Non-goals

leader election（阶段 2）；Agent 多 Hub 发现；读扩展/流复制运维；SQLite 移除；连接池独立配置键（走 PG URL 参数）。
