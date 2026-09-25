---
components: [qdrant]
description: ArkFlow documentation page.
---

# Qdrant

The `qdrant` output upserts each batch's rows as [Qdrant](https://qdrant.tech/) points over the REST API. Each row becomes one point: the vector comes from a `FixedSizeList(Float32)`/`List(Float32)` column (typically produced by the [embedding processor](../2-processors/embedding.md)), an optional id column keys the point, and every remaining column lands in the point payload.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `qdrant` |
| url | string | yes | — | Qdrant base URL (e.g. `http://localhost:6333`). |
| collection | string | yes | — | Target collection name. |
| vector_field | string | no | `embedding` | Column holding the vector. |
| id_field | string | no | — | Column used as the point id (unsigned integer or string). When omitted, the console generates a random UUID v4 per row — redelivery inserts duplicates, so prefer an id column for at-least-once idempotency. |
| payload_fields | array | no | all other columns | Columns included in the point payload. |
| api_key | string | no | — | Sent as `Authorization: Bearer`. Supports [secret references](/docs/reference/configuration#secret-references). |
| timeout_ms | integer | no | `30000` | HTTP request timeout. |
| retry_count | integer | no | `3` | Retry attempts for connection errors and 5xx responses; 4xx responses fail immediately. |
| headers | map | no | — | Extra HTTP headers. |

## Semantics

- Writes use `PUT /collections/{collection}/points?wait=true` — one request per batch.
- Upserts are idempotent per point id: with an `id_field`, redelivery overwrites the same point (at-least-once friendly); without one, a fresh UUID is generated per row, so every attempt inserts a new point.
- The vector's dimension must match the collection's; mismatches are returned by Qdrant as a 4xx and fail the batch (routed to `error_output` when configured).
- Requests to loopback endpoints (a local Qdrant instance) always bypass the system proxy.

## Example

```yaml validate=fragment wrap=output
output:
  type: "qdrant"
  url: "http://localhost:6333"
  collection: "documents"
  vector_field: "embedding"
  id_field: "doc_id"
```

### Complete Pipeline Example

```yaml validate=full
logging:
  level: info

streams:
  - id: docs-upserts
    input:
      type: memory
    pipeline:
      processors:
        - type: json_to_arrow
    output:
      type: qdrant
      url: "http://localhost:6333"
      collection: "documents"
      vector_field: "embedding"
      id_field: "doc_id"
```
