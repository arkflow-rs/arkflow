## Why

README 主打「无缝集成 AI 能力」（`README.md:16-19`），但全仓库无任何 AI 组件：`crates/arkflow-plugin/src/processor/` 仅 6 种（batch/json/protobuf/python/sql/vrl），processor 目录无 llm/embedding 命中，Cargo 依赖无 ML/LLM 库——这是 PLANNING.md 1.2 判定的「宣传与实现脱节最严重处 = 最大差异化机会」。2026 行业主流叙事是 streaming + AI（RisingWave 已全面转向），用户最基本的 AI 管道需求是**把文本流向量化并入向量库**（RAG 摄取/语义检索），当前 ArkFlow 完全做不了。`reqwest` 已在 workspace 与 arkflow-plugin 依赖中（`crates/arkflow-plugin/Cargo.toml:43`），HTTP output 已有 auth/retry/codec 先例（`crates/arkflow-plugin/src/output/http.rs`），列式 batch 天然适合批量 embedding 调用。

## What Changes

- 新增 processor `embedding`（`crates/arkflow-plugin/src/processor/embedding.rs`）：
  - 调 OpenAI 兼容 `/embeddings` API（`api_base`/`model`/`api_key`，`api_key` 可用 `${env:...}` secret 引用），把输入 batch 的指定文本列（`field`）批量向量化；
  - 向量作为新列 `target_field`（默认 `embedding`）追加到 batch，类型 `FixedSizeList(Float32, dim)`；
  - 批内分块（`batch_size`）控制单次请求行数；API 失败返回错误（走既有 error_output 语义）。
- 新增 output `qdrant`（`crates/arkflow-plugin/src/output/qdrant.rs`）：
  - 通过 Qdrant REST API 批量 upsert points；`vector_field` 指定向量列，`id_field` 指定点 id 列（缺省用行号），其余（或 `payload_fields` 指定的）列进 payload；
  - `url`/`collection`/`api_key`（secret 引用可用）/`timeout_ms`/`retry_count`，可选 codec 编码 payload。
- 两个组件注册 builder + metadata schema，接入 `components list/show/schema` 与文档体系。

## Capabilities

### New Capabilities

- `ai-vector-ingestion`: embedding processor 与 qdrant output 的配置形状、批量语义、Arrow 向量列类型、错误语义与鉴权（secret 引用兼容）。

### Modified Capabilities

<!-- 无既有能力需求级变更。 -->

## Impact

- `crates/arkflow-plugin/src/processor/`：新增 `embedding.rs`，`mod.rs` init 接线。
- `crates/arkflow-plugin/src/output/`：新增 `qdrant.rs`，`mod.rs` init 接线。
- `crates/arkflow-plugin/Cargo.toml`：无新增依赖（reqwest/json 已在）。
- 文档：两个组件页（components front matter）、两份 README 组件清单、2 个示例 YAML + manifest 注册、inventory 重新生成。
- README AI 叙事首次有实现支撑；为后续 LLM processor / 本地推理（PLANNING 7.3-3/4）打前站。

## Non-goals

- 不做本地推理（ONNX/candle）、LLM 补全/对话 processor、流式 embedding RAG 查询侧——后续立项。
- 不做 milvus/pgvector/weaviate 等 output——qdrant 先行，REST 形状可复制。
- 不做 embedding 缓存、维度校验对账（collection dim 不匹配由 Qdrant 侧报错透传）、token 计费可观测。
- 不引入任何 ML 推理依赖；仅 HTTP 调用。
