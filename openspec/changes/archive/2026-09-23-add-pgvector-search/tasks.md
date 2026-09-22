# Tasks: add-pgvector-search

## 1. pgvector_search processor 实现

- [x] 1.1 新建 `crates/arkflow-plugin/src/processor/pgvector_search.rs`：`PgVectorSearchProcessorConfig`（serde default 齐全）+ builder 构建期校验 + `connect_lazy` 连接池 + metadata schema 注册；`processor/mod.rs` init 接线
- [x] 1.2 实现向量列提取与 pgvector 文本序列化（复用 output pgvector 模式）；实现检索 SQL 生成（度量算子映射 `<=>`/`<->`/`<#>`、payload 列可选、LIMIT）
- [x] 1.3 实现有界并发逐行查询与行→JSON 映射（id 文本、distance、payload 解析）、buffered 保序回填、空 batch 直通
- [x] 1.4 离线单测：三度量 SQL 文本断言、payload 禁用形状、LIMIT、行→JSON 映射（含禁 payload）、null/空向量与类型错、空 batch、构建期校验
- [x] 1.5 `#[ignore]` 真库集成测试：建表 → 写入（raw SQL）→ 检索断言最近邻与行序

## 2. 文档与示例

- [x] 2.1 组件文档页 `docs/docs/components/2-processors/pgvector-search.md`（表形状对称性、三种度量口径说明、validate 分类标记、绝对路由链接）
- [x] 2.2 双 README `README_COMPONENTS:processor` 清单各加一条
- [x] 2.3 示例 `examples/pgvector_rag_query_pipeline.yaml`（memory → json_to_arrow → embedding → pgvector_search → stdout），注册 example-manifest
- [x] 2.4 `ARKFLOW_REGENERATE_DOCS=1` 重新生成 inventory/config-schema 与 component-inventory.md

## 3. 全量验证与归档

- [x] 3.1 `cargo test --workspace --all-targets` 全绿；`cargo clippy --workspace --all-targets` 无新告警；`pnpm docs:check` 通过
- [x] 3.2 对照 specs/pgvector-search 场景逐条核对；`openspec validate add-pgvector-search` 通过
- [x] 3.3 同步主 spec `openspec/specs/pgvector-search/spec.md`、归档 change、更新 PLANNING.md 7.3-3 进展、提交
