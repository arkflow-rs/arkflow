# Capability: AI Vector Ingestion

## Purpose

Define the configuration, batching semantics, Arrow vector column typing, error semantics, and authentication of the AI vector-ingestion components: the `embedding` processor (OpenAI-compatible embeddings API) and the `qdrant` output (REST upsert), which together form the minimal streaming RAG / semantic-search ingestion pipeline. (Purpose derived from the `add-ai-vector-ingestion` change; refine as the capability evolves.)

## Requirements

### Requirement: embedding processor 的配置与请求语义

`embedding` processor SHALL 接受配置 `api_base`（必填）、`model`（必填）、`api_key`（可选，支持 secret 引用）、`field`（必填，输入文本列名）、`target_field`（可选，默认 `embedding`）、`batch_size`（可选，默认 32）、`timeout_ms`（可选，默认 30000）与 `headers`（可选 map）。processor SHALL 把输入 batch 的 `field` 列按 `batch_size` 分块，逐块调用 `POST {api_base}/embeddings`（body `{"model": model, "input": [texts]}`），并对每行把响应中的向量追加为 `target_field` 列，类型 `FixedSizeList(Float32, dim)`。

#### Scenario: 批量向量化追加列

- **WHEN** 输入 batch 含 3 行 `text` 列（`a`、`b`、`c`），embedding API 对 `input: ["a","b","c"]` 返回 3 个 4 维向量
- **THEN** 输出 batch 在原列之外含 `embedding` 列，类型 `FixedSizeList(Float32, 4)`，第 i 行向量与 `data[i].embedding` 相等

#### Scenario: batch_size 分块顺序调用

- **WHEN** 输入 batch 含 5 行且 `batch_size` 为 2
- **THEN** processor 发出 3 次请求（行数 2、2、1），顺序与行序一致，全部成功后输出 5 行

#### Scenario: api_key 走 Bearer 头且支持 secret 引用

- **WHEN** 配置 `api_key: "${env:EMBED_KEY}"` 且环境变量值为 `sk-test`
- **THEN** 每个请求携带 `Authorization: Bearer sk-test`

#### Scenario: 非 Utf8 输入列报错

- **WHEN** `field` 指向的列不是字符串类型
- **THEN** process 返回 `Error::Process`，错误指明列名与实际类型

#### Scenario: API 错误透传

- **WHEN** embedding API 返回非 2xx（如 401）
- **THEN** process 返回错误，错误信息包含状态码与响应体片段，batch 走 error_output 语义

#### Scenario: 维度不一致报错

- **WHEN** 同一响应内向量长度不同
- **THEN** process 返回 `Error::Process`，不产出半成品 batch

#### Scenario: 空 batch 直通

- **WHEN** 输入 batch 为 0 行
- **THEN** process 返回 `ProcessResult::None`，不发起 HTTP 请求

### Requirement: qdrant output 的配置与写入语义

`qdrant` output SHALL 接受配置 `url`（必填）、`collection`（必填）、`vector_field`（可选，默认 `embedding`）、`id_field`（可选）、`payload_fields`（可选列表，缺省为除向量/ID 列外的全部列）、`api_key`（可选）、`timeout_ms`（可选，默认 30000）、`retry_count`（可选，默认 3）与 `headers`。output SHALL 以 `PUT /collections/{collection}/points?wait=true` 批量 upsert：每行一个 point，`vector` 取 `vector_field` 列（`FixedSizeList(Float32)` 或 `List(Float32)`），`id` 取 `id_field` 列（配置了就必须存在且为 integer/utf8；缺省省略 id 由 Qdrant 生成），`payload` 由 payload 列按行构成 JSON 对象。

#### Scenario: 批量 upsert 携带向量与 payload

- **WHEN** 输入 batch 含 2 行（`embedding` 向量列、`text` 字符串列），配置仅 `url`/`collection`
- **THEN** 一次 PUT 请求体含 2 个 points，每个 point 的 `vector` 为对应向量、`payload` 为 `{"text": ...}`

#### Scenario: id_field 指定列

- **WHEN** 配置 `id_field: doc_id` 且列存在
- **THEN** 每个 point 的 `id` 取该列对应行值（integer 原样，utf8 作为字符串 id）

#### Scenario: id_field 缺失报错

- **WHEN** 配置 `id_field: doc_id` 但 batch 无该列
- **THEN** write 返回错误，指明缺失列名

#### Scenario: 鉴权头

- **WHEN** 配置 `api_key`（含 secret 引用解析后的值）
- **THEN** 请求携带 `Authorization: Bearer <值>`；未配置则不携带

#### Scenario: 5xx 重试与 4xx 不重试

- **WHEN** Qdrant 先后返回 500 与 400
- **THEN** 500 按 `retry_count` 重试后仍失败才返回错误；400 立即返回错误不重试

### Requirement: 组件注册与文档体系一致性

两个组件 SHALL 以 `embedding` / `qdrant` 注册 builder 与 metadata schema（`components list/show/schema` 可见），SHALL 各有带 `components:` front matter 的文档页、双 README 组件清单条目、注册进 example-manifest 的示例 YAML；生成的 inventory SHALL 与注册一致。

#### Scenario: registry 一致性

- **WHEN** 运行 workspace 测试（registry_consistency、docs_inventory_snapshot）与 `pnpm docs:check`
- **THEN** 全部通过，无需手工编辑生成文件（用 `ARKFLOW_REGENERATE_DOCS=1` 再生成）

### Requirement: llm 流式补全（SSE）

`stream=true` 时 llm processor SHALL 在请求体携带 `"stream": true`，并将响应按 OpenAI 兼容 SSE 帧解析：`data: {json}` 增量的 `choices[0].delta.content` SHALL 按到达顺序拼接为该行最终文本，`data: [DONE]` SHALL 终止；缺失 `[DONE]` 时 SHALL 以响应结束为终止。输出列语义与非流式一致（每行一个完整补全）。非法 SSE 数据 SHALL 以 Error::Process 失败。

#### Scenario: 增量聚合为整行文本

- **WHEN** mock API 以三个 content 增量与 `[DONE]` 回应 stream 请求
- **THEN** 输出行的 response 列为全部增量拼接的完整文本，请求体含 `"stream":true`

### Requirement: llm tool use（function calling）

`tools` 配置 SHALL 原样透传到请求体。配置 `tool_calls_column` 时，processor SHALL 追加该列：非流式响应取 `choices[0].message.tool_calls`，流式响应 SHALL 按 tool call index 合并分片（id/name 取首个非空值，arguments 按序拼接）后序列化为 JSON 数组文本；无 tool_calls 时该列 SHALL 为空字符串。未配置 `tool_calls_column` 时 SHALL 不追加列（向后兼容）。

#### Scenario: 非流式 tool_calls 落列

- **WHEN** 配置 tools 与 tool_calls_column 且响应 message 含 tool_calls
- **THEN** 该列为 tool_calls 的 JSON 文本且请求体透传 tools

#### Scenario: 流式 tool_call 分片按 index 合并

- **WHEN** 流式响应以两个 arguments 分片（同一 index）传递 tool call
- **THEN** tool_calls 列为合并后参数完整 JSON 的数组文本
