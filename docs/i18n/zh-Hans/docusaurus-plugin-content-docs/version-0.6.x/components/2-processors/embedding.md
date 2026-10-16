---
components: [embedding]
description: ArkFlow Embedding 处理器组件。
---

# Embedding

`embedding` 处理器(processor)通过 OpenAI 兼容的 embeddings API,对流入批次(Batch)的一个文本列做批量向量化,并将向量追加为 `FixedSizeList(Float32, dim)` 列。任何支持 OpenAI `/embeddings` 协议的端点均可使用:OpenAI、Azure OpenAI(通过 `headers`)、vLLM、Ollama 或 TEI 兼容网关。

它与 [Qdrant 输出](/zh-Hans/docs/components/outputs/qdrant)搭配,构成流式 RAG / 语义搜索摄取管道。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `embedding` |
| api_base | string | yes | — | API 基础 URL;请求路径为 `{api_base}/embeddings`。 |
| model | string | yes | — | 嵌入模型名称(如 `text-embedding-3-small`)。 |
| api_key | string | no | — | 以 `Authorization: Bearer` 形式发送。支持 [Secret 引用](/zh-Hans/docs/reference/configuration#secret-references)。 |
| field | string | yes | — | 待向量化的输入 UTF-8 列。各行必须非 null。 |
| target_field | string | no | `embedding` | 追加的向量列名称。 |
| batch_size | integer | no | `32` | 每个 HTTP 请求的最大行数。 |
| timeout_ms | integer | no | `30000` | HTTP 请求超时。 |
| headers | map | no | — | 额外的 HTTP 头(如 Azure 的 `api-key`)。 |

:::note
发往回环(loopback)端点的请求(例如本地 vLLM/Ollama 实例)始终绕过系统代理。
:::

## 错误语义

API 调用失败(非 2xx)或响应不一致(向量数量或维度不匹配、输入文本为 null)都会以处理错误的形式暴露,因此在配置了 `error_output` 的流中,该批次会流向 `error_output`。该处理器不会重试嵌入请求——重试会使 API 开销成倍增加;请改用 `error_output` 或上游重投递。

## 示例

```yaml validate=fragment wrap=processors
- type: "embedding"
  api_base: "https://api.openai.com/v1"
  model: "text-embedding-3-small"
  api_key: "${env:OPENAI_API_KEY:-}"
  field: "text"
  batch_size: 32
```

### 完整管道示例

```yaml validate=full
logging:
  level: info

streams:
  - id: docs-embedder
    input:
      type: kafka
      brokers:
        - broker1.example.com:9092
      topics:
        - documents
      consumer_group: arkflow-embedder
      start_from_latest: false
    pipeline:
      thread_num: 4
      processors:
        - type: json_to_arrow
        - type: embedding
          api_base: "https://api.openai.com/v1"
          model: "text-embedding-3-small"
          api_key: "${env:OPENAI_API_KEY:-}"
          field: "text"
    output:
      type: stdout
```
