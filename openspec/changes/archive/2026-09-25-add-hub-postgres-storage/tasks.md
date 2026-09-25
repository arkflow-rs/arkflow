# Tasks: add-hub-postgres-storage

## 1. Backend 抽象 + SQLite 搬移（零行为变化）

- [x] 1.1 `storage.rs` → `storage/` 目录拆分：`mod.rs`（records/error/command/actor/trait/枚举）、`sqlite.rs`（SqliteBackend，现有逻辑原样）、保持 `crate::storage::*` 引用面不变。
- [x] 1.2 `StorageBackend` async trait 定义（与命令一一对应）；actor match 臂改 `.await`；`ControlPlaneStore` 枚举委托（SQLite 单变体起步）。
- [x] 1.3 28 个存储测试转 `#[tokio::test]` + `.await`，全部通过。

## 2. PostgreSQL 实现

- [x] 2.1 workspace `Cargo.toml` 增加 `sqlx 0.8`（runtime-tokio + postgres only）；arkflow-server 引入。
- [x] 2.2 `postgres.rs`：PgPool 连接 + `SELECT 1` 启动探测 + 17 表幂等 DDL（BIGINT/IDENTITY/COLLATE "C"/partial unique index）。
- [x] 2.3 `impl StorageBackend for PostgresBackend`：全命令实现（`?N`→`$N` 重写共享 SQL 文本、`INSERT OR IGNORE` 改写、u64↔i64 cast、`RETURNING`、23505 冲突映射）。
- [x] 2.4 `StorageError::Pool(#[from] sqlx::Error)`。

## 3. 入口分派

- [x] 3.1 `ControlPlaneStore::open` 变 async + scheme 分派；`bin/arkflow-server.rs` 适配；lib.rs/hub.rs 测试调用点适配。

## 4. 契约测试参数化

- [x] 4.1 断言体抽 `assert_contract(backend)`；SQLite 全量；PG 门控 `ARKFLOW_TEST_POSTGRES_URL`（无则 skip）。
- [x] 4.2 PG DDL/SQL 文本离线断言（幂等可重复执行、占位符重写正确性）。

## 5. 迁移工具

- [x] 5.1 `migrate` 子命令：外键序逐表分块拷贝、IDENTITY setval、行数对账、非零退出。
- [x] 5.2 迁移测试（内存 SQLite → 门控 PG）。

## 6. 文档与归档

- [x] 6.1 `docs/docs/control-plane/deploy.md`（en/zh）：PG 配置、迁移步骤、停写要求。
- [x] 6.2 `cargo test --workspace --all-targets` + clippy 全绿。
