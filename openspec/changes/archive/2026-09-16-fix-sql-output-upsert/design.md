## Context

`sql` output（`crates/arkflow-plugin/src/output/sql.rs`）当前按 batch 逐行取值、经 sqlx `QueryBuilder` 生成一条多值 `INSERT` 写 MySQL/PostgreSQL。`init()` 注册的组件元数据 schema（sql.rs:441-458）宣称 `connection`/`table`/`batch_size`/`upsert`/`upsert_keys`，与真实配置 `SqlOutputConfig`（`output_type`/`table_name`，sql.rs:138-143）不符；upsert 无任何实现，MySQL/PG 均裸 INSERT（`execute_insert`，sql.rs:64-134）。

约束：配置字段新增必须向后兼容（serde default）；本项目 EOS 叙事（PLANNING.md 方向②）依赖 SQL upsert 作为 L1 幂等吸收手段，语义须与「已 commit 序列的重复写入由 sink 幂等兜底」一致——同一主键重复写入时覆盖旧值。

## Goals / Non-Goals

**Goals:**
- 实装 upsert（MySQL `ON DUPLICATE KEY UPDATE` / PG `ON CONFLICT DO UPDATE`），冲突时覆盖非 key 列。
- 构建期校验 `upsert=true` ⇒ 非空 `upsert_keys`；写入期校验 key 列存在。
- 元数据 schema 与 `SqlOutputConfig` 真实字段一致，修正描述与 example。
- 文档页、示例、生成物（inventory/config-schema）同步。

**Non-Goals:**
- 其他 output 的幂等适配；SQLite 方言；连接池/并发写；事务语义变更。

## Decisions

1. **配置字段形态：顶层 `upsert: bool` + `upsert_keys: [string]`（serde default）**——与现有元数据宣称的命名保持一致（`upsert`/`upsert_keys`），使「虚标的字段」变成真实字段而非再发明一套命名；挂在 `SqlOutputConfig` 顶层而非 `output_type` 内部，两方言共用同一配置面。备选：放在各方言 config 内（拒绝：强迫用户在 mysql/postgres 间迁移字段，无收益）。
2. **冲突子句经 `QueryBuilder::push()` 追加原始 SQL 片段**——`push_values` 生成 VALUES 后按方言追加 `ON DUPLICATE KEY UPDATE`/`ON CONFLICT (...) DO UPDATE SET`；非 key 列逐一 `col = VALUES(col)`（MySQL）/`col = EXCLUDED.col`（PG）。key 列不出现在 SET 中（覆盖 key 自身无意义且 PG 要求 conflict target 与 SET 分离）。备选：改用 sqlx 事务 + 先 UPDATE 后 INSERT（拒绝：多次往返、非原子，违背幂等单语句语义）。
3. **key 列校验放在写入期**（`upsert_keys` ⊆ batch schema 列名）而非 build 期——build 期拿不到 batch schema；写入期首条消息即可发现配置错误并返回明确错误。build 期只校验 `upsert=true ⇒ 非空 upsert_keys`。
4. **MySQL 方言 `sqlx::MySql` QueryBuilder 的占位符兼容性**——`ON DUPLICATE KEY UPDATE` 子句不含绑定参数（纯标识符），避免 sqlx 占位符重排问题；PG 同理（`EXCLUDED.col` 为标识符引用）。
5. **测试策略：离线断言生成 SQL 文本**——`query_builder.build().sql()` 可在不连接数据库的情况下取得最终 SQL 字符串，单测断言两方言含/不含 upsert 的语句形态与校验错误路径；不需要测试容器。

## Risks / Trade-offs

- [MySQL `ON DUPLICATE KEY UPDATE` 在唯一键冲突以外的场景（如触发器联动）语义宽泛] → 文档明示冲突判定依赖表的主键/唯一索引，属用户表设计责任。
- [`VALUES(col)` 语法在 MySQL 8.0.20+ 被标记 deprecated（仍可用）] → 采用仍被广泛支持的 `VALUES(col)` 形式；如未来 MySQL 移除，迁移成本集中在单处 SQL 片段。
- [upsert 改变「同主键重复」的可见结果（追加行 → 覆盖）] → 仅在用户显式 `upsert: true` 时发生，默认行为不变。
- [元数据 schema 修正会让此前按虚标字段写配置的用户在 `--validate` 下暴露] → 该配置本就无法工作（serde 忽略未知字段后缺 `output_type`/`table_name` 必失败），修正只是让失败提前且明确。

## Migration Plan

纯增量字段 + 元数据修正，无迁移；回滚即还原单文件与文档。部署顺序无约束。

## Open Questions

无。
