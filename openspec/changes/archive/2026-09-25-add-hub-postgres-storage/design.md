# Design: add-hub-postgres-storage

完全遵循已归档的评审级设计 `openspec/specs/hub-ha/postgres-storage-design.md`（现状事实表、Backend 抽象、17 表映射规则、一致性语义、配置面、迁移工具、测试策略、任务分解、风险、决策记录均在其中）。本文只记录实施层面的补充决策：

1. **模块布局**：`storage.rs`（单文件 5,204 行）拆为 `storage/` 目录——`mod.rs`（records/StorageError/StorageCommand/StorageActor/StorageBackend trait/ControlPlaneStore 枚举与分派/re-export）、`sqlite.rs`（SqliteBackend，现有 rusqlite 逻辑原样搬移）、`postgres.rs`（PostgresBackend + 幂等 DDL）、`migrate_tool.rs`（一次性迁移）。路径 `crate::storage::X` 的外部引用面不变。
2. **open 签名**：`ControlPlaneStore::open` 变 async（PG 连接探测 `SELECT 1` 需 await）；二进制入口与测试调用点同步适配。`in_memory()` 保持同步（仅 SQLite）。scheme 分派按设计 §6：`postgres://`/`postgresql://` 前缀 → PG，其余按 SQLite 路径。
3. **actor 适配**：match 臂由同步调用改为 `backend.method(...).await`——FIFO 顺序不变性由 mpsc 单消费者 + 逐条 await 保持（设计 §3）。
4. **测试转化**：28 个 `#[test]` 转 `#[tokio::test]` + `.await`，断言体抽为 `async fn assert_contract(backend: &impl StorageBackend)` 参数化双后端；PG 门控变量 `ARKFLOW_TEST_POSTGRES_URL`。`table_exists`/`index_exists` 为 SQLite 专属测试助手，留在 `sqlite.rs`。
5. **PG SQL 复用策略**：运行时把 SQLite 语句的 `?N` 占位符重写为 `$N`（`INSERT OR IGNORE` 单独改写为 `ON CONFLICT DO NOTHING`），使 PG 实现与 SQLite 实现共享同一 SQL 文本，消除逐条转写的笔误面；u64/i64 绑定统一 cast 为 i64（PG 无无符号）。
6. **sqlx 版本**：workspace 新增 `sqlx = "0.8"`，default-features off + `runtime-tokio` + `postgres`——不拉入 sqlite/any 特性（SQLite 仍走 rusqlite，避免双驱动）。
