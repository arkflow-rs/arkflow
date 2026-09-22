# Tasks: add-ai-vector-ingestion

## 1. embedding processor

- [x] 1.1 新建 `crates/arkflow-plugin/src/processor/embedding.rs`：`EmbeddingProcessorConfig`（serde default 齐全）+ builder + metadata schema 注册；`processor/mod.rs` init 接线
- [x] 1.2 实现列提取与分块：`field` 列 Utf8 校验、`batch_size` 分块、null 行报错、空 batch → `ProcessResult::None`
- [x] 1.3 实现 HTTP 调用与响应装配：`POST {api_base}/embeddings`、Bearer 鉴权、`headers` 覆盖、超时；响应 `data[i].embedding` → `FixedSizeList(Float32, dim)` 追加列（dim 不一致报错）
- [x] 1.4 进程内 mock server 测试：happy path（3 行/4 维追加列断言）、batch_size 分块次数与顺序、Bearer 头断言、非 Utf8 列报错、401 透传、dim 不一致、空 batch 直通

## 2. qdrant output

- [x] 2.1 新建 `crates/arkflow-plugin/src/output/qdrant.rs`：`QdrantOutputConfig` + builder + metadata schema 注册；`output/mod.rs` init 接线
- [x] 2.2 实现 points 装配与 upsert：`vector_field` 列提取（Fixed/List(Float32)）、`id_field` 语义（缺省省略 id）、payload 列 JSON 化、`PUT /collections/{collection}/points?wait=true`
- [x] 2.3 实现鉴权与重试：Bearer 头、`headers` 覆盖、连接类错误/5xx 按 `retry_count` 重试、4xx 立即失败
- [x] 2.4 mock server 测试：upsert 请求体断言（vector/payload/id）、id_field 缺失报错、鉴权头、5xx 重试与 4xx 不重试

## 3. 文档与示例

- [x] 3.1 组件文档页 `docs/docs/components/2-processors/embedding.md` 与 `docs/docs/components/3-outputs/qdrant.md`（components front matter + validate 分类标记的 yaml 块）
- [x] 3.2 双 README `README_COMPONENTS` 清单各加一条（type-name + 描述）
- [x] 3.3 示例 `examples/embedding_qdrant_pipeline.yaml`（memory → embedding → qdrant）与 `examples/embedding_processor.yaml`，注册 example-manifest
- [x] 3.4 `ARKFLOW_REGENERATE_DOCS=1 cargo test -p arkflow-plugin --test docs_inventory_snapshot` 重新生成 inventory/config-schema

## 4. 全量验证

- [x] 4.1 `cargo test --workspace --all-targets` 全绿
- [x] 4.2 `cargo clippy --workspace --all-targets` 无新告警
- [x] 4.3 `pnpm docs:check` 通过
- [x] 4.4 对照 specs/ai-vector-ingestion 场景逐条核对；`openspec validate add-ai-vector-ingestion` 通过
