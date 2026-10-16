---
components: [milvus]
description: ArkFlow Milvus 输出组件。
---

# Milvus

`milvus` 输出(Output)通过 REST v2 vectordb API(Milvus **2.4+**)将每个批次(Batch)的行 UPSERT 到 [Milvus](https://milvus.io/) 集合(Collection)中。它与 [embedding 处理器](/zh-Hans/docs/components/processors/embedding)配合使用:Float32 列表列成为向量,其余所有列打包进一个 JSON payload 字段,可选的 id 列作为行的键。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `milvus` |
| url | string | yes | — | Milvus 基础 URL(例如 `http://localhost:19530`)。 |
| collection | string | yes | — | 目标集合(schema 必须已存在)。 |
| vector_field | string | no | `embedding` | 存放向量的字段(Float32 组成的 `FixedSizeList`/`List`)。 |
| id_field | string | yes | — | 存放行 id 的字段(整数或字符串)。必填:REST v2 upsert API 要求每行携带主键,auto-id 集合不受支持(缺失时 builder 拒绝)。 |
| payload_field | string | no | `payload` | 接收其余所有列(按行打包为对象)的 JSON 字段。设为 `""` 可禁用。 |
| api_key | string | no | — | 以 `Authorization: Bearer` 形式发送;Milvus 惯例为 `<user>:<password>`。支持 [secret 引用](/zh-Hans/docs/reference/configuration#secret-references)。 |
| timeout_ms | integer | no | `30000` | HTTP 请求超时。 |
| retry_count | integer | no | `0` | 连接错误与 5xx 响应的重试次数(从 100ms 起指数退避)。超过 1000 行的批次无论如何都会拆分为多个请求。 |
| headers | map | no | — | 额外的 HTTP 头。 |

## 集合 schema

运行前请先创建集合;payload 字段必须是 JSON 类型(或启用动态字段,并让 `payload_field` 落在动态键上):

```bash
# via curl, once:
curl -s http://localhost:19530/v2/vectordb/collections/create -d '{
  "collectionName": "documents",
  "dimension": 1536
}'
```

至多 1,000 行的批次产生一个携带全部行的 upsert 请求;更大的批次按序切分为多个 1,000 行请求:

```json
{"collectionName": "documents", "data": [
  {"doc_id": 1, "embedding": [0.1, "..."], "payload": {"text": "..."}}
]}
```

:::note
Milvus 会以 HTTP 200 加响应体中非零 `code` 的形式报告失败——本输出将其视为错误,并把 `code` 与 `message` 暴露出来。`id_field` 为必填项:重新投递会覆盖同一行,对至少一次(at-least-once)语义友好(auto-id 集合不受支持)。发往环回端点的请求始终绕过系统代理。
:::

## 示例

```yaml validate=fragment wrap=output
output:
  type: "milvus"
  url: "http://localhost:19530"
  collection: "documents"
  vector_field: "embedding"
  id_field: "doc_id"
  api_key: "${env:MILVUS_CREDENTIALS:-}"
```

### 完整流水线示例

```yaml validate=full
logging:
  level: info

streams:
  - id: docs-to-milvus
    input:
      type: memory
    pipeline:
      thread_num: 4
      processors:
        - type: json_to_arrow
        - type: embedding
          api_base: "http://localhost:9997/v1"
          model: "text-embedding-3-small"
          api_key: "${env:OPENAI_API_KEY:-}"
          field: "text"
    output:
      type: milvus
      url: "http://localhost:19530"
      collection: "documents"
      vector_field: "embedding"
      id_field: "doc_id"
```
