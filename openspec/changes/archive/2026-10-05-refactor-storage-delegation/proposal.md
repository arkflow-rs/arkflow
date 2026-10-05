# Proposal: refactor-storage-delegation

## Why

`crates/arkflow-server/src/storage/` 已膨胀到 13.7k 行，其中 mod.rs 的六层手写委托样板（StorageCommand 枚举 63 变体、`nack()` 63 臂、`label()` 63 臂、`dispatch()` 62 臂、StorageActor 63 个代理方法、`impl StorageBackend for ControlPlaneStore` 66 个两臂 match）约 2,600 行是纯机械重复——新增一个存储操作需要改 6 处，漏改靠穷尽 match 的编译错误兜底。逐臂审计（2026-10-05）确认全部符合三条固定约定（String→`&str`、`Option<String>`→`Option<&str>`/`as_deref()`、其余按值），仅 1 个方法重命名特例（`ReadHubLeaseSnapshot`↔`hub_lease_snapshot`）。同时两个后端均无 schema 版本号（幂等 DDL 无演进路径，库版本新于二进制时静默漂移）。

## What Changes

- **`storage_ops!` 声明式宏**：单张操作表（label/变体名/方法名/返回类型/fenced 分类/字段清单含传递模式）统一生成六层委托（枚举、nack、label、dispatch、actor 代理、ControlPlaneStore 的 StorageBackend impl）。**行为零变化**：字段传递约定、fenced 分类、`ReadHubLeaseSnapshot` 重命名、4 个查询方法的 `Option<impl Into<String>>` 参数风格、`#[allow(clippy::too_many_arguments)]`、既有 doc 注释全部逐位保留。样板 ~2,600 行 → 表 ~330 行 + 宏 ~260 行；新增操作从 6 处编辑降为 1 条表项。
- **DB schema 版本号**：SQLite `PRAGMA user_version`、PostgreSQL `cp_schema_meta` 表，当前版本 1。启动 DDL 后校验：库版本新于二进制 → 拒绝启动并指明版本；等于/缺失 → 盖章。`migrate` 工具复制后对目标库盖章并校验源库版本不新于二进制。
- **trait `StorageBackend` 保持手写**（契约本体，含文档与 3 个无变体的 fence 方法）。

## Capabilities

### New Capabilities

（无）

### Modified Capabilities

- `hub-ha`: 存储后端新增 schema 版本契约——库版本新于二进制 MUST 拒绝启动（新库旧进程防护），migrate 工具 MUST 校验并转移版本戳。

## Impact

- `crates/arkflow-server/src/storage/mod.rs`（六层样板 → 宏生成；预计 -2,000 行净减）
- `crates/arkflow-server/src/storage/sqlite.rs`、`postgres.rs`（DDL 后版本盖章与校验）
- `crates/arkflow-server/src/storage/migrate_tool.rs`（版本校验）
- 安全网：43 处双后端契约测试 + 52 个 mod 测试 + lib.rs 76 个测试全量回归；SQLite/PG 行为零变化。
