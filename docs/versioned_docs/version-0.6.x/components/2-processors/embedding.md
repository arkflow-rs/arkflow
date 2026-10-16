---
components: [embedding]
description: ArkFlow documentation page.
---

# Embedding

The `embedding` processor batch-embeds a text column of the incoming batch through an OpenAI-compatible embeddings API and appends the vectors as a `FixedSizeList(Float32, dim)` column. Any endpoint that speaks the OpenAI `/embeddings` protocol works: OpenAI, Azure OpenAI (via `headers`), vLLM, Ollama, or TEI-compatible gateways.

It pairs with the [Qdrant output](../3-outputs/qdrant.md) to form a streaming RAG / semantic-search ingestion pipeline.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `embedding` |
| api_base | string | yes | — | API base URL; the request path is `{api_base}/embeddings`. |
| model | string | yes | — | Embedding model name (e.g. `text-embedding-3-small`). |
| api_key | string | no | — | Sent as `Authorization: Bearer`. Supports [secret references](/docs/reference/configuration#secret-references). |
| field | string | yes | — | Input UTF-8 column to embed. Rows must be non-null. |
| target_field | string | no | `embedding` | Name of the appended vector column. |
| batch_size | integer | no | `32` | Maximum rows per HTTP request. |
| timeout_ms | integer | no | `30000` | HTTP request timeout. |
| headers | map | no | — | Extra HTTP headers (e.g. Azure's `api-key`). |

:::note
Requests to loopback endpoints (a local vLLM/Ollama instance, for example)
always bypass the system proxy.
:::

## Error semantics

A failed API call (non-2xx), an inconsistent response (vector count or dimension mismatch, null input text) surfaces as a processing error, so the batch flows to the stream's `error_output` when configured. The processor does not retry embedding requests — retries would multiply API spend; rely on `error_output` or upstream redelivery instead.

## Example

```yaml validate=fragment wrap=processors
- type: "embedding"
  api_base: "https://api.openai.com/v1"
  model: "text-embedding-3-small"
  api_key: "${env:OPENAI_API_KEY:-}"
  field: "text"
  batch_size: 32
```

### Complete Pipeline Example

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
