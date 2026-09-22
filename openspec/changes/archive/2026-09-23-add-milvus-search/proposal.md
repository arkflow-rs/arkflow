## Why

AI 切片的检索侧已有 qdrant（`vector_search`）与 pgvector（`pgvector_search`）两个后端，Milvus 只有摄取（output `milvus`）没有查询——第三大主流开源向量库的用户无法在流内完成 RAG 闭环。Milvus REST v2 的 search 端点天然支持**批量查询**（`data` 数组携带多个查询向量、响应按序返回每组命中），与 ArkFlow 的列式批模型完美对齐——比 qdrant/pgvector 检索的逐行请求更高效。全部基建可复用：milvus output 的 code!=0 判败语义、Bearer 鉴权、loopback 代理绕过与 mock 测试模式；`vector_search` 的结果 JSON 列语义。零新增依赖。

## What Changes

- 新增 processor `milvus_search`（`crates/arkflow-plugin/src/processor/milvus_search.rs`）：
  - 读取输入 batch 的 `vector_field` 列（Fixed/List Float32），发起**单次** `POST {url}/v2/vectordb/entities/search`：`data` 数组按行携带查询向量、`limit=top_k`、`outputFields=[id_field?, payload_field?]`、`searchParams.metricType`；
  - 响应 `data[i]`（第 i 行的命中数组）逐行序列化为 JSON 数组文本，追加为 `target_field`（默认 `matches`）列，元素含 `id`（配置了 `id_field` 时）、`distance` 与 `payload`（配置了 `payload_field` 时）；
  - 复用 milvus output 的判败语义：HTTP 非 2xx 或 `code != 0` 均为失败；响应 `data` 数组与输入行数不一致时报错；
  - 空 batch 直通；向量列缺失/类型错/null/空向量报 `Error::Process`。
- 组件以 `milvus_search` 注册 builder + metadata schema，接入文档体系。

## Capabilities

### New Capabilities

- `milvus-search`: milvus_search processor 的配置形状、批量检索请求/响应语义、结果 JSON 列、错误语义与鉴权。

### Modified Capabilities

<!-- 无既有能力需求级变更：milvus output 不动。 -->

## Impact

- `crates/arkflow-plugin/src/processor/`：新增 `milvus_search.rs`，`mod.rs` init 接线；无新增依赖。
- 文档：组件页、双 README 组件清单、Milvus 版 RAG 查询示例 + manifest 注册、inventory 重新生成。
- Milvus 成为继 qdrant/pgvector 之后第三个「摄取+检索」双完整后端。

## Non-goals

- 不做过滤器（filter 表达式下推）、分区/partition 指定、range search（radius）——按需后补。
- 不做混合检索（BM25）、重排序；不做查询侧流式响应。
- 不做 Qdrant 检索组件的批量化改造（其 API 无批量形态，维持逐行 + 并发）。
- 不引入 Milvus gRPC SDK。
