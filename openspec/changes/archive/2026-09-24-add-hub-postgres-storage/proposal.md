## Why

Hub HA 阶段 1（蓝图：openspec/specs/hub-ha/postgres-storage-design.md）：Hub 控制面存储可选 PostgreSQL，为多 Hub 部署与 DB 租约选主（阶段 2）铺路。实施在堆叠分支上进行，不触碰 PR #1247 的审查范围；合入后 PR 自动 retarget。

## What Changes

- `ControlPlaneBackend` 枚举（Sqlite(ControlPlaneStore) / Postgres(PgStore)）：StorageActor 的 FIFO 顺序保持，match 臂改为 backend 分派 await。
- `PgStore`：sqlx PgPool + 17 张 `cp_*` 表的幂等 PG DDL（AUTOINCREMENT→IDENTITY、`?N`→`$N`、`INSERT OR IGNORE`→`ON CONFLICT DO NOTHING`）；52 个命令对应的 sqlx 实现；`last_insert_rowid` 点改 `RETURNING`。
- 入口：`ARKFLOW_HUB_STORAGE` 按 scheme 分派（`postgres://`→PG，裸路径→SQLite，零破坏）。
- 测试：契约套件对 SQLite 与 live PG（`ARKFLOW_TEST_POSTGRES_URL` 门控）双跑，覆盖每个后端方法；无 PG 时 PG 套件 skip。
- `arkflow-server migrate --from sqlite:<path> --to postgres:<url>` 一次性迁移（外键序、分块事务、行数对账）。

## Capabilities

### Modified Capabilities

- `hub-ha`: 阶段 1 落地——PostgreSQL 存储后端成为可选 backend。

## Impact

- `crates/arkflow-server/src/storage.rs`（backend 枚举/分派）、新文件 `pg_store.rs`、`bin/arkflow-server.rs`（scheme 分派 + migrate 子命令）。
- deploy 文档 en/zh；PLANNING HA 路线更新。

## Non-goals

- leader election（阶段 2）；Agent 多 Hub 发现；移除 SQLite；读扩展运维方案。
