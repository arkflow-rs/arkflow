# Capability: Milvus Output

## Purpose

Define the configuration, REST v2 upsert semantics (including Milvus's HTTP-200-with-non-zero-code failure mode), vector/payload/id field mapping, and error semantics of the `milvus` output: writing streaming batches into Milvus collections. (Purpose derived from the `add-milvus-output` change; refine as the capability evolves.)

## Requirements

### Requirement: milvus output 的配置与请求语义

`milvus` output SHALL 接受配置 `url`（必填，Milvus 基地址）、`collection`（必填）、`vector_field`（可选，默认 `embedding`）、`id_field`（可选）、`payload_field`（可选，默认 `payload`，显式置空禁用）、`api_key`（可选，`Authorization: Bearer` 原样携带）、`timeout_ms`（可选，默认 30000）、`headers`（可选 map）与 `retry_count`（可选，默认 0）。每批 SHALL 以 `POST {url}/v2/vectordb/entities/upsert` 写入，`data` 数组每行一个对象：`{"<id_field>": 值, "<vector_field>": [Float32...], "<payload_field>": {JSON 对象}}`——`id_field` 未配置时省略 id 键（auto-id），`payload_field` 置空时省略 payload 键。不超过 1000 行的批 SHALL 以单请求发送；超过 1000 行的批 SHALL 按 1000 行切片顺序发送多个请求。瞬时失败（连接错误与 5xx 应答）SHALL 按 `retry_count` 以 100ms 起步的指数退避重试，重试耗尽 SHALL 返回错误。

#### Scenario: 全字段 upsert 请求体

- **WHEN** 配置 `collection: docs`、`id_field: doc_id`，输入 2 行（向量列 2 维、`text` 列）
- **THEN** 请求路径为 `/v2/vectordb/entities/upsert`，`collectionName` 为 `docs`，`data[0]` 含 `doc_id`、`embedding: [f32...]`、`payload: {"text": ...}`，且 id/vector/payload 键名与配置一致

#### Scenario: auto-id 省略 id 键

- **WHEN** 未配置 `id_field`
- **THEN** 每行对象不含 id 键，仅含向量与 payload

#### Scenario: 禁用 payload

- **WHEN** 配置 `payload_field: ""`
- **THEN** 每行对象不含 payload 键

#### Scenario: 批量多行单请求

- **WHEN** 输入 batch 含 3 行
- **THEN** 单次请求 `data` 数组含 3 个行对象

#### Scenario: 大批量按切片分请求

- **WHEN** 输入 batch 含 2500 行
- **THEN** 依次发送 3 个 upsert 请求，`data` 分别含 1000、1000、500 个行对象，顺序与行序一致

#### Scenario: 瞬时失败按退避重试

- **WHEN** upsert 请求遇 503 且 `retry_count: 2`，第三次尝试成功
- **THEN** 输出成功返回，两次重试间隔分别约为 100ms 与 200ms

### Requirement: Milvus 应答判败语义

HTTP 2xx 且响应体 `code == 0`（或 `code` 缺失）SHALL 视为成功；HTTP 2xx 且 `code != 0` SHALL 返回含 `code` 与 `message` 的 `Error::Process`；HTTP 非 2xx SHALL 返回含状态码与截断响应体的 `Error::Process`。向量列缺失、类型不是 Float32 列表、null 向量、空向量 SHALL 返回指明列名与行号的 `Error::Process`。空 batch SHALL 成功且不发起请求。构建期 SHALL 拒绝 `url`/`collection` 为空的配置。

#### Scenario: HTTP 200 且 code 为非零

- **WHEN** Milvus 返回 HTTP 200 且体为 `{"code": 100, "message": "collection not found"}`
- **THEN** write 返回错误，信息包含 `100` 与 `collection not found`

#### Scenario: HTTP 200 且 code 为零

- **WHEN** 响应体为 `{"code": 0, "data": {...}}`
- **THEN** write 成功

#### Scenario: 非 2xx 透传

- **WHEN** 返回 HTTP 401
- **THEN** write 返回错误，信息含 401 与响应体片段

#### Scenario: null 向量行

- **WHEN** 向量列某行为 null
- **THEN** write 返回错误并指明列名与行号，不发起请求

#### Scenario: 空 batch 直通

- **WHEN** 输入 batch 为 0 行
- **THEN** write 成功且不发起请求

### Requirement: 组件注册与文档体系一致性

组件 SHALL 以 `milvus` 注册 builder 与 metadata schema（`components list/show/schema` 可见），SHALL 有带 `components:` front matter 的文档页（含最低版本与 DDL 说明）、双 README 组件清单条目、注册进 example-manifest 的示例 YAML；生成的 inventory SHALL 与注册一致。

#### Scenario: registry 一致性

- **WHEN** 运行 workspace 测试（registry_consistency、docs_inventory_snapshot）与 `pnpm docs:check`
- **THEN** 全部通过，生成文件无需手工编辑
