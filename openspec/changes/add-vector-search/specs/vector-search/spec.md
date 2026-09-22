# vector-search 变更（Delta）

## ADDED Requirements

### Requirement: vector_search processor 的配置与检索请求语义

`vector_search` processor SHALL 接受配置 `url`（必填，qdrant 基地址）、`collection`（必填）、`vector_field`（可选，默认 `embedding`）、`target_field`（可选，默认 `matches`）、`top_k`（可选，默认 5）、`score_threshold`（可选 number）、`concurrency`（可选，默认 4）、`api_key`（可选，支持 secret 引用）、`timeout_ms`（可选，默认 30000）与 `headers`（可选 map）。processor SHALL 读取输入 batch 的 `vector_field` 列，对每行发起一次 `POST {url}/collections/{collection}/points/search`（body `{"vector": [...], "limit": top_k, "with_payload": true}`，`score_threshold` 仅在配置时出现），并以有界并发（在途 ≤ concurrency）执行、按行序回填结果。

#### Scenario: 检索结果按行序回填为 JSON 列

- **WHEN** 输入 batch 含 2 行向量列，qdrant 对各行返回 Top-2 命中
- **THEN** 输出 batch 在原列之外含 `matches` Utf8 列，第 i 行为第 i 行命中的 JSON 数组文本，保留 `id`/`score`/`payload` 键与得分降序

#### Scenario: 请求体参数形状

- **WHEN** 配置 `top_k: 3` 且未配置 `score_threshold`
- **THEN** 请求体为 `{"vector": [...], "limit": 3, "with_payload": true}`，不含 `score_threshold` 键；配置 `score_threshold: 0.8` 时以数值出现

#### Scenario: 有界并发且保序

- **WHEN** 输入 6 行、`concurrency` 为 2、响应延迟扰动
- **THEN** 在途请求数不超过 2，输出仍按行序回填且全部成功

#### Scenario: api_key 鉴权与 secret 引用

- **WHEN** 配置 `api_key: "${env:QDRANT_KEY}"`（解析后 `sk-q`）
- **THEN** 每个请求携带 `Authorization: Bearer sk-q`；未配置时不携带鉴权头

#### Scenario: 空 batch 直通

- **WHEN** 输入 batch 为 0 行
- **THEN** process 返回 `ProcessResult::None`，不发起 HTTP 请求

### Requirement: vector_search processor 的错误语义

以下情形 SHALL 返回指明原因的 `Error::Process`（批次走 error_output 语义）：`vector_field` 缺失、类型不是 Float32 列表、null 向量、空向量；API 非 2xx（错误含状态码与截断响应体）；响应缺 `result` 数组。构建期 SHALL 拒绝 `url`/`collection`/`vector_field` 为空与 `top_k`/`concurrency` 为 0 的配置。

#### Scenario: 非 2xx 透传

- **WHEN** qdrant 返回 404（集合不存在）
- **THEN** process 返回错误，信息包含 404 与响应体片段

#### Scenario: 响应缺 result

- **WHEN** 响应体不含 `result` 数组
- **THEN** process 返回错误，不产出空结果列

#### Scenario: null 向量行

- **WHEN** `vector_field` 列某行为 null
- **THEN** process 返回错误并指明行号

#### Scenario: 构建期校验

- **WHEN** 配置 `top_k: 0`、`concurrency: 0` 或必填字段为空
- **THEN** builder 返回 `Error::Config`，不创建组件

### Requirement: 组件注册与文档体系一致性

组件 SHALL 以 `vector_search` 注册 builder 与 metadata schema（`components list/show/schema` 可见），SHALL 有带 `components:` front matter 的文档页、双 README 组件清单条目、注册进 example-manifest 的完整 RAG 管道示例 YAML；生成的 inventory SHALL 与注册一致。

#### Scenario: registry 一致性

- **WHEN** 运行 workspace 测试（registry_consistency、docs_inventory_snapshot）与 `pnpm docs:check`
- **THEN** 全部通过，生成文件无需手工编辑
