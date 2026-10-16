---
components: [milvus_search]
description: ArkFlow Milvus Search 处理器组件。
---

# Milvus Search

`milvus_search` 处理器(processor)在 [Milvus](https://milvus.io/) 集合(2.4+,REST v2)中搜索每行向量的 top-k 最近邻(nearest-neighbor),并把匹配结果作为 JSON 数组文本列追加。与 Qdrant 和 pgvector 搜索处理器相同,逐行发起 REST 请求(单元素查询数组),采用有界、保序的并发。请与 [embedding 处理器](/zh-Hans/docs/components/processors/embedding)以及 [Milvus 输出](/zh-Hans/docs/components/outputs/milvus)搭配使用——后者写入的字段形状与该处理器读取的完全相同。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `milvus_search` |
| url | string | yes | — | Milvus 基础 URL(例如 `http://localhost:19530`)。 |
| collection | string | yes | — | 要搜索的集合。 |
| vector_field | string | no | `embedding` | 持有查询向量的批次列(Float32 的 `FixedSizeList`/`List`)。 |
| target_field | string | no | `matches` | 追加的匹配结果列名称。 |
| id_field | string | no | — | 作为匹配 id 返回的集合字段;auto-id 集合请省略。 |
| payload_field | string | no | `payload` | 作为 payload 对象包含在每个匹配中的集合字段。设为 `""` 可禁用。 |
| metric | string | no | `COSINE` | 度量类型(`COSINE`、`L2`、`IP`);必须与集合 schema 一致。 |
| top_k | integer | no | `5` | 每行的邻居数量。 |
| api_key | string | no | — | 以 `Authorization: Bearer` 形式发送(Milvus 约定:`<user>:<password>`)。支持 [Secret 引用](/zh-Hans/docs/reference/configuration#secret-references)。 |
| timeout_ms | integer | no | `30000` | HTTP 请求超时。 |
| concurrency | integer | no | `4` | 最大并发搜索请求数。 |
| headers | map | no | — | 额外的 HTTP 头。 |

## 结果结构

匹配结果会被规范化为与其他搜索处理器相同的键——`id`(配置了
`id_field` 时)、`distance`(Milvus 的原始值)以及 `payload`(所配置字段的对象):

```json
[{"id": 42, "distance": 0.12, "payload": {"text": "..."}}]
```

:::note
失败情形(HTTP 错误、Milvus `code != 0`、结果分组数与输入行数不一致、查询向量为
null/空)会使整个批次失败并进入 `error_output`。该处理器不会重试。发往回环(loopback)端点的请求始终绕过系统代理。
:::

## 示例

```yaml validate=fragment wrap=processors
- type: "milvus_search"
  url: "http://localhost:19530"
  collection: "documents"
  vector_field: "embedding"
  id_field: "doc_id"
  metric: "COSINE"
  top_k: 5
```

### 完整 RAG 查询管道

```yaml validate=full
logging:
  level: info

streams:
  - id: milvus-rag-query
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
        - type: milvus_search
          url: "http://localhost:19530"
          collection: "documents"
          id_field: "doc_id"
          top_k: 5
    output:
      type: stdout
```
