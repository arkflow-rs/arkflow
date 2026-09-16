## 1. 配置与校验

- [x] 1.1 `SqlOutputConfig` 新增 `upsert: Option<bool>`、`upsert_keys: Option<Vec<String>>`（serde default）
- [x] 1.2 build 校验：`upsert=true` 时 `upsert_keys` 必须非空，否则返回配置错误（含单测）

## 2. upsert 实现

- [x] 2.1 `execute_insert` 按方言追加冲突子句：MySQL `ON DUPLICATE KEY UPDATE`（非 key 列 `col=VALUES(col)`）、PG `ON CONFLICT (...) DO UPDATE SET col=EXCLUDED.col`
- [x] 2.2 写入期校验 `upsert_keys` ⊆ batch schema 列名，缺失时返回明确错误（含单测）
- [x] 2.3 离线单测断言两方言含/不含 upsert 的生成 SQL 文本（`build().sql()`）

## 3. 元数据与文档

- [x] 3.1 `init()` 元数据 schema 改为真实字段（output_type/table_name/upsert/upsert_keys），修正描述与内置 example
- [x] 3.2 更新 `docs/docs/components/3-outputs/sql.md`：补 upsert 字段行、语义说明与示例
- [x] 3.3 新增 `examples/sql_output_upsert.yaml` 并注册 `docs/reference/example-manifest.json`

## 4. 生成物与回归

- [x] 4.1 `ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot` 重新生成 inventory/config-schema（并重跑 `generate-component-inventory.mjs` 同步 .md）
- [x] 4.2 `cargo test -p arkflow-plugin` + `cargo clippy -p arkflow-plugin` 通过；example 经 `examples_validate` 离线校验通过
