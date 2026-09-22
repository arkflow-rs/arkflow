---
components: [milvus]
description: ArkFlow documentation page.
---

# Milvus

The `milvus` output upserts each batch's rows into a [Milvus](https://milvus.io/) collection over the REST v2 vectordb API (Milvus **2.4+**). It pairs with the [embedding processor](/docs/components/2-processors/embedding): a Float32 list column becomes the vector, every remaining column packs into a JSON payload field, and an optional id column keys the row.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `milvus` |
| url | string | yes | — | Milvus base URL (e.g. `http://localhost:19530`). |
| collection | string | yes | — | Target collection (schema must already exist). |
| vector_field | string | no | `embedding` | Field carrying the vector (`FixedSizeList`/`List` of Float32). |
| id_field | string | no | — | Field carrying the row id (integer or string). When omitted the id key is left out — use an auto-id collection. |
| payload_field | string | no | `payload` | JSON field receiving every remaining column as a per-row object. Set to `""` to disable. |
| api_key | string | no | — | Sent as `Authorization: Bearer`; Milvus convention is `<user>:<password>`. Supports [secret references](/docs/reference/configuration#secret-references). |
| timeout_ms | integer | no | `30000` | HTTP request timeout. |
| headers | map | no | — | Extra HTTP headers. |

## Collection schema

Create the collection before running; the payload field must be a JSON type
(or enable the dynamic field and keep `payload_field` on the dynamic key):

```bash
# via curl, once:
curl -s http://localhost:19530/v2/vectordb/collections/create -d '{
  "collectionName": "documents",
  "dimension": 1536
}'
```

Each batch produces one upsert request carrying all rows:

```json
{"collectionName": "documents", "data": [
  {"doc_id": 1, "embedding": [0.1, "..."], "payload": {"text": "..."}}
]}
```

:::note
Milvus reports failures as HTTP 200 with a non-zero `code` in the body —
this output treats that as an error and surfaces `code` plus `message`.
With `id_field` set, redelivery overwrites the same row (at-least-once
friendly); without it every attempt inserts a new row (auto-id). Requests
to loopback endpoints always bypass the system proxy.
:::

## Example

```yaml validate=fragment wrap=output
output:
  type: "milvus"
  url: "http://localhost:19530"
  collection: "documents"
  vector_field: "embedding"
  id_field: "doc_id"
  api_key: "${env:MILVUS_CREDENTIALS:-}"
```

### Complete Pipeline Example

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
