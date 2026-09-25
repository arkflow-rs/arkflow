# Tasks: add-milvus-search

## 1. milvus_search processor 实现

- [x] 1.1 新建 `crates/arkflow-plugin/src/processor/milvus_search.rs`：`MilvusSearchProcessorConfig`（serde default 齐全）+ builder 构建期校验 + metadata schema 注册；`processor/mod.rs` init 接线
- [x] 1.2 实现向量列提取与批量请求装配：`data` 数组按行携带向量、`limit`、`outputFields`（按 id_field/payload_field 组装）、`searchParams.metricType`
- [x] 1.3 实现响应映射：`data[i]` 与输入行对应校验、命中归一化键名（id/distance/payload）、JSON 数组文本列追加、Bearer 鉴权与 loopback 代理绕过
- [x] 1.4 mock server 测试：请求体形状（含 metricType 映射与 outputFields 两态）、按行序归一化回填、auto-id 无 id 键、code!=0 判败、HTTP 404、data 错位、null 向量、空 batch、构建期校验

## 2. 文档与示例

- [x] 2.1 组件文档页 `docs/docs/components/2-processors/milvus-search.md`（最低版本、与 output 的字段对称性、validate 分类标记、绝对路由链接）
- [x] 2.2 双 README `README_COMPONENTS:processor` 清单各加一条
- [x] 2.3 示例 `examples/milvus_rag_query_pipeline.yaml`，注册 example-manifest
- [x] 2.4 `ARKFLOW_REGENERATE_DOCS=1` 重新生成 inventory/config-schema 与 component-inventory.md

## 3. 全量验证与归档

- [x] 3.1 `cargo test --workspace --all-targets` 全绿；`cargo clippy --workspace --all-targets` 无新告警；`pnpm docs:check` 通过
- [x] 3.2 对照 specs/milvus-search 场景逐条核对；`openspec validate add-milvus-search` 通过
- [x] 3.3 同步主 spec `openspec/specs/milvus-search/spec.md`、归档 change、更新 PLANNING.md 7.3-3 进展、提交
