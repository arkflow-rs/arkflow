## MODIFIED Requirements

### Requirement: vector_search processor 的配置与检索请求语义

`vector_search` processor SHALL 接受配置 `url`（必填，qdrant 基地址）、`collection`（必填）、`vector_field`（可选，默认 `embedding`）、`target_field`（可选，默认 `matches`）、`top_k`（可选，默认 5）、`score_threshold`（可选 number）、`concurrency`（可选，默认 4）、`api_key`（可选，支持 secret 引用）、`timeout_ms`（可选，默认 30000）与 `headers`（可选 map）。processor SHALL 读取输入 batch 的 `vector_field` 列，对每行发起一次 `POST {url}/collections/{collection}/points/search`（body `{"vector": [...], "limit": top_k, "with_payload": true}`，`score_threshold` 仅在配置时出现），并以有界并发（在途 ≤ concurrency）执行、按行序回填结果。`collection` SHALL 作为单个 URL 路径段百分号编码后拼入请求路径：保留字符（RFC 3986 unreserved：字母、数字、`-`、`.`、`_`、`~`）逐字节不变，其余字符以大写 `%XX` 序列编码，使含 `/`、`?`、`#`、空格等字符的集合名仍寻址同一 collection。

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

#### Scenario: 集合名特殊字符按路径段编码

- **WHEN** 配置 `collection: "docs/2024?x y"` 且输入 1 行
- **THEN** 请求行以 `POST {url}/collections/docs%2F2024%3Fx%20y/points/search ` 开头，保留字符与常规集合名（如 `documents-2.x`）的 URL 逐字节不变
