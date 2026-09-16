## Why

`sql` output 的组件元数据与实现严重脱节：

1. **元数据宣称的配置字段不存在**：`crates/arkflow-plugin/src/output/sql.rs:441-458` 的 JSON schema 宣称 `connection`/`table`/`batch_size` 字段且 `additionalProperties: false`，但真实配置结构体 `SqlOutputConfig`（`crates/arkflow-plugin/src/output/sql.rs:138-143`）只有 `output_type`/`table_name`——按该 schema 校验会拒绝一切合法配置；IDE 补全（`components show/schema`）与文档会引导用户写出无效配置。
2. **upsert 纯属虚标**：元数据宣称 `upsert`/`upsert_keys`（sql.rs:449-450）、描述写着 "Supports upsert and transaction management"（sql.rs:441），但配置结构体没有这两个字段，`execute_insert`（sql.rs:64-134）对 MySQL/PostgreSQL 均只生成裸 `INSERT INTO`。PLANNING.md 中「SQL L1 复用现有 upsert（零代码）」的 EOS 叙事建立在不存在的能力上；对幂等 sink 而言重复写入无法被吸收，这正是方向②生产级可靠性的缺口。

## What Changes

- `SqlOutputConfig` 新增 `upsert: Option<bool>` 与 `upsert_keys: Option<Vec<String>>`（serde default，向后兼容，现有配置零改动）。
- 实装 upsert 写入：MySQL 生成 `INSERT ... ON DUPLICATE KEY UPDATE`，PostgreSQL 生成 `INSERT ... ON CONFLICT (...) DO UPDATE SET`，冲突更新作用于非 key 列。
- build 期校验：`upsert=true` 时必须提供非空 `upsert_keys`。
- 组件元数据 schema 对齐真实配置（`output_type`/`table_name`/`upsert`/`upsert_keys`），修正描述与内置 example。
- 文档与示例同步：更新 `docs/docs/components/3-outputs/sql.md`，新增 `examples/sql_output_upsert.yaml` 并注册 example manifest，重新生成 inventory/config-schema。

## Capabilities

### New Capabilities

- `sql-output`: sql output 的批量插入与 upsert 行为契约——配置契约（字段与校验）、批量 INSERT 语义、两方言 upsert 语义、元数据与实现一致性。

### Modified Capabilities

<!-- 无：现有 specs/ 中没有 sql output 相关 capability；本 change 新增。 -->

## Impact

- 代码：`crates/arkflow-plugin/src/output/sql.rs`（配置结构体、`execute_insert`、`init()` 元数据）。
- 文档：`docs/docs/components/3-outputs/sql.md`、`examples/sql_output_upsert.yaml`、`docs/reference/example-manifest.json`、生成物 `docs/reference/component-inventory.json` 与 `docs/static/config-schema.json`（经快照测试重新生成）。
- 兼容性：纯增量——新增字段均有 serde default，未配置 upsert 时行为与现状逐字节一致；无破坏性变更。
- 无新依赖。

## Non-goals

- 其他 output（mongodb/redis/influxdb 等）的幂等写入适配。
- SQLite 方言支持（代码中已注释掉，不在本次启用）。
- 连接池/并发写、批量发布、事务管理语义变更。
