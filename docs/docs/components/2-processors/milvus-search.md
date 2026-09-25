---
components: [milvus_search]
description: ArkFlow documentation page.
---

# Milvus Search

The `milvus_search` processor searches a [Milvus](https://milvus.io/) collection (2.4+, REST v2) for the top-k nearest neighbors of each row's vector and appends the matches as a JSON array text column. Unlike the Qdrant and pgvector search processors (one request per row), Milvus accepts a **batched query array** — the whole batch is one request, with responses mapped back by position. Pair it with the [embedding processor](/docs/components/processors/embedding) and the [Milvus output](/docs/components/outputs/milvus), which writes the same field shape this processor reads.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `milvus_search` |
| url | string | yes | — | Milvus base URL (e.g. `http://localhost:19530`). |
| collection | string | yes | — | Collection to search. |
| vector_field | string | no | `embedding` | Batch column holding the query vector (`FixedSizeList`/`List` of Float32). |
| target_field | string | no | `matches` | Name of the appended matches column. |
| id_field | string | no | — | Collection field returned as the match id. Omit for auto-id collections. |
| payload_field | string | no | `payload` | Collection field included in each match as a payload object. Set to `""` to disable. |
| metric | string | no | `COSINE` | Metric type (`COSINE`, `L2`, `IP`); must match the collection schema. |
| top_k | integer | no | `5` | Number of neighbors per row. |
| api_key | string | no | — | Sent as `Authorization: Bearer` (Milvus convention: `<user>:<password>`). Supports [secret references](/docs/reference/configuration#secret-references). |
| timeout_ms | integer | no | `30000` | HTTP request timeout. |
| headers | map | no | — | Extra HTTP headers. |

## Result shape

Matches are normalized to the same keys as the other search processors —
`id` (when `id_field` is configured), `distance` (Milvus's raw value),
and `payload` (the configured field's object):

```json
[{"id": 42, "distance": 0.12, "payload": {"text": "..."}}]
```

:::note
Failures (HTTP errors, Milvus `code != 0`, result groups not matching the
input row count, null/empty query vectors) fail the batch into
`error_output`. The processor does not retry. Requests to loopback
endpoints always bypass the system proxy.
:::

## Example

```yaml validate=fragment wrap=processors
- type: "milvus_search"
  url: "http://localhost:19530"
  collection: "documents"
  vector_field: "embedding"
  id_field: "doc_id"
  metric: "COSINE"
  top_k: 5
```

### Complete RAG Query Pipeline

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
