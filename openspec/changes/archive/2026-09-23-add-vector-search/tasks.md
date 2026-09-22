# Tasks: add-vector-search

## 1. vector_search processor 实现

- [x] 1.1 新建 `crates/arkflow-plugin/src/processor/vector_search.rs`：`VectorSearchProcessorConfig`（serde default 齐全）+ builder 构建期校验 + metadata schema 注册；`processor/mod.rs` init 接线
- [x] 1.2 实现向量列提取（Fixed/List Float32 → Vec<f32>，null/空报错含行号）与请求体装配（limit/with_payload，score_threshold 缺省省略）
- [x] 1.3 实现有界并发检索：`buffered(concurrency)` 保序回填、Bearer 鉴权、headers、loopback 代理绕过、非 2xx/缺 result 错误、结果 JSON 数组文本列追加
- [x] 1.4 mock server 测试：行序回填与 JSON 形状断言、请求体参数（top_k/threshold 有无）、并发上限与保序、鉴权头/无鉴权、404 透传、缺 result、null 向量、空 batch、构建期校验

## 2. 文档与示例

- [x] 2.1 组件文档页 `docs/docs/components/2-processors/vector-search.md`（components front matter、validate 分类标记、绝对路由链接、结果 JSON 形状说明）
- [x] 2.2 双 README `README_COMPONENTS:processor` 清单各加一条
- [x] 2.3 示例 `examples/rag_query_pipeline.yaml`（memory → json_to_arrow → embedding → vector-search → llm → stdout，完整 RAG 查询链路），注册 example-manifest
- [x] 2.4 `ARKFLOW_REGENERATE_DOCS=1` 重新生成 inventory/config-schema 与 component-inventory.md

## 3. 全量验证与归档

- [x] 3.1 `cargo test --workspace --all-targets` 全绿；`cargo clippy --workspace --all-targets` 无新告警；`pnpm docs:check` 通过
- [x] 3.2 对照 specs/vector-search 场景逐条核对；`openspec validate add-vector-search` 通过
- [x] 3.3 同步主 spec `openspec/specs/vector-search/spec.md`、归档 change、更新 PLANNING.md 7.3-3 进展、提交
