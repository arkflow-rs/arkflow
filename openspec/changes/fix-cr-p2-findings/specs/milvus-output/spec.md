## MODIFIED Requirements

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
