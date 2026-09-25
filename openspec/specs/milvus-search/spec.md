# Capability: Milvus Search

## Purpose

Define the configuration, batched retrieval request/response semantics, normalized result column shape, and error semantics of the `milvus_search` processor: in-stream top-k similarity search against Milvus collections over the REST v2 API, completing the Milvus RAG loop alongside the embedding processor and the milvus output. (Purpose derived from the `add-milvus-search` change; refine as the capability evolves.)

## Requirements

### Requirement: milvus_search processor 的配置与批量检索请求语义

`milvus_search` processor SHALL 接受配置 `url`（必填，Milvus 基地址）、`collection`（必填）、`vector_field`（可选，默认 `embedding`，输入向量列）、`target_field`（可选，默认 `matches`）、`id_field`（可选）、`payload_field`（可选，默认 `payload`，显式置空禁用）、`metric`（可选，`COSINE | L2 | IP`，默认 `COSINE`）、`top_k`（可选，默认 5）、`api_key`（可选，`Authorization: Bearer` 原样携带）、`timeout_ms`（可选，默认 30000）与 `headers`（可选 map）。对输入 batch SHALL 发起**单次** `POST {url}/v2/vectordb/entities/search`：`data` 数组按行携带查询向量、`limit` 为 `top_k`、`outputFields` 按 `id_field`/`payload_field` 配置组装、`searchParams.metricType` 为配置度量。

#### Scenario: 批量单请求与请求体形状

- **WHEN** 输入 batch 含 2 行向量列，配置 `collection: docs`、`top_k: 3`、`id_field: doc_id`、`payload_field: payload`
- **THEN** 单次请求体含 `collectionName: "docs"`、`data` 数组 2 个查询向量、`limit: 3`、`outputFields` 含 `doc_id` 与 `payload`、`searchParams.metricType: "COSINE"`

#### Scenario: metricType 可选值映射

- **WHEN** `metric` 分别配置为 `L2` 与 `IP`
- **THEN** `searchParams.metricType` 分别为 `L2` 与 `IP`

#### Scenario: auto-id 集合的 outputFields

- **WHEN** 未配置 `id_field`
- **THEN** `outputFields` 不含 id 字段，命中对象不含 `id` 键

### Requirement: 响应映射与错误语义

响应 `data[i]` SHALL 与输入第 i 行对应，每个命中 SHALL 以归一化键名（`id`/`distance`/`payload`）序列化为 JSON 数组文本并按行序回填为 `target_field` Utf8 列；命中 `distance` 取 Milvus 返回值，`payload` 取配置字段的对象。`data` 缺失、非数组或长度与输入行数不一致 SHALL 返回 `Error::Process`。HTTP 非 2xx 与 HTTP 200 + `code != 0` SHALL 均视为失败（分别含状态码+截断体、code+message）；`code` 缺失宽容为成功。向量列缺失、类型不是 Float32 列表、null 向量、空向量 SHALL 返回指明列名与行号的 `Error::Process`。空 batch SHALL 成功且不发起请求。构建期 SHALL 拒绝 `url`/`collection` 为空与 `top_k: 0` 的配置。

#### Scenario: 按行序回填归一化命中

- **WHEN** 输入 2 行，响应 `data` 为两行各自的 2 个命中（含 `doc_id`、`distance`、`payload` 字段）
- **THEN** 输出 `matches` 列第 i 行为第 i 行命中的 JSON 数组文本，每个命中含归一化 `id`/`distance`/`payload` 键

#### Scenario: data 长度错位报错

- **WHEN** 输入 2 行但响应 `data` 只有 1 组命中
- **THEN** process 返回 `Error::Process`，不产出错位结果

#### Scenario: HTTP 200 且 code 非零

- **WHEN** 响应体为 `{"code": 100, "message": "collection not found"}`
- **THEN** process 返回错误，信息包含 `100` 与 `collection not found`

#### Scenario: null 向量行

- **WHEN** `vector_field` 列某行为 null
- **THEN** process 返回错误并指明行号，不发起请求

#### Scenario: 空 batch 直通

- **WHEN** 输入 batch 为 0 行
- **THEN** process 返回 `ProcessResult::None`，不发起请求

#### Scenario: 构建期校验

- **WHEN** 配置 `top_k: 0` 或 `url`/`collection` 为空
- **THEN** builder 返回 `Error::Config`

### Requirement: 组件注册与文档体系一致性

组件 SHALL 以 `milvus_search` 注册 builder 与 metadata schema（`components list/show/schema` 可见），SHALL 有带 `components:` front matter 的文档页、双 README 组件清单条目、注册进 example-manifest 的 Milvus 版 RAG 查询示例 YAML；生成的 inventory SHALL 与注册一致。

#### Scenario: registry 一致性

- **WHEN** 运行 workspace 测试（registry_consistency、docs_inventory_snapshot）与 `pnpm docs:check`
- **THEN** 全部通过，生成文件无需手工编辑
