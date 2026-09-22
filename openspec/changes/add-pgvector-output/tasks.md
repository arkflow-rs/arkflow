# Tasks: add-pgvector-output

## 1. pgvector output 实现

- [x] 1.1 新建 `crates/arkflow-plugin/src/output/pgvector.rs`：`PgVectorOutputConfig`（serde default 齐全）+ builder 构建期校验 + metadata schema 注册；`output/mod.rs` init 接线
- [x] 1.2 实现列提取与序列化：向量列（FixedSizeList/List Float32 → `[v1,v2]` 文本，null/空报错含行号）、payload 打包（arrow-json 逐行 JSON，排除 vector/id 列，可禁用）、id 列（Int64/Int32/Utf8，负数报错）
- [x] 1.3 实现 SQL 生成：QueryBuilder 手工行元组 + `::vector`/`::jsonb` cast、`ON CONFLICT ... DO UPDATE`（EXCLUDED）、批量多行单条 INSERT；`connect()` 建 PgPool
- [x] 1.4 离线单测：SQL 文本断言（全列/无 id/禁 payload 三形状 + cast 位置 + 冲突子句 + 多行）、向量序列化、payload 打包、null/空向量报错、空 batch 直通、构建期校验
- [x] 1.5 `#[ignore]` 真库集成测试：docker pgvector/pgvector 建表 → 插入 → 冲突更新断言（文档说明运行方式）

## 2. 文档与示例

- [x] 2.1 组件文档页 `docs/docs/components/3-outputs/pgvector.md`（components front matter + validate 分类标记、绝对路由链接、DDL 前置说明与 #[ignore] 测试运行方式）
- [x] 2.2 双 README `README_COMPONENTS:output` 清单各加一条
- [x] 2.3 示例 `examples/embedding_pgvector_pipeline.yaml`（memory → json_to_arrow → embedding → pgvector），注册 example-manifest
- [x] 2.4 `ARKFLOW_REGENERATE_DOCS=1` 重新生成 inventory/config-schema 与 component-inventory.md

## 3. 全量验证与归档

- [x] 3.1 `cargo test --workspace --all-targets` 全绿；`cargo clippy --workspace --all-targets` 无新告警；`pnpm docs:check` 通过
- [x] 3.2 对照 specs/pgvector-output 场景逐条核对；`openspec validate add-pgvector-output` 通过
- [x] 3.3 同步主 spec `openspec/specs/pgvector-output/spec.md`、归档 change、更新 PLANNING.md 7.3-3 进展、提交
