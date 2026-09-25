---
components: [pgvector_search]
description: ArkFlow documentation page.
---

# pgvector Search

The `pgvector_search` processor runs one nearest-neighbor SELECT per row against a PostgreSQL table with the [pgvector](https://github.com/pgvector/pgvector) extension and appends the matches as a JSON array text column. The table shape mirrors what the [pgvector output](/docs/components/outputs/pgvector) writes — `(id, embedding vector(n), payload jsonb)` — so ingestion and retrieval work on the same table out of the box.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `pgvector_search` |
| url | string | yes | — | Postgres connection string. Supports [secret references](/docs/reference/configuration#secret-references). |
| table | string | yes | — | Table to search (must already exist). |
| vector_field | string | no | `embedding` | Batch column holding the query vector (`FixedSizeList`/`List` of Float32). |
| target_field | string | no | `matches` | Name of the appended matches column. |
| id_column | string | no | `id` | Table column returned as the match id (as text). |
| vector_column | string | no | `embedding` | Table column holding the stored vectors. |
| payload_column | string | no | `payload` | jsonb column included in each match as a payload object. Set to `""` to disable. |
| metric | string | no | `cosine` | Distance operator: `cosine` (`<=>`), `l2` (`<->`), `inner_product` (`<#>`). |
| top_k | integer | no | `5` | Number of neighbors per row. |
| concurrency | integer | no | `4` | Maximum in-flight queries; results are always filled back in row order. |
| max_connections | integer | no | `4` | Connection pool size. |
| timeout_ms | integer | no | `30000` | Pool acquire timeout. |

## Distance semantics

`distance` is the raw pgvector operator result, nearest-first via
`ORDER BY ... <op> $1`:

| metric | operator | distance meaning |
|--------|----------|------------------|
| `cosine` | `<=>` | cosine distance, `0` = identical direction, range `[0, 2]` |
| `l2` | `<->` | Euclidean distance, `0` = identical |
| `inner_product` | `<#>` | negative inner product (smaller = more similar) |

Each match object carries `id` (as text), `distance`, and `payload` (the
parsed jsonb object) — the JSON text feeds directly into an
[LLM processor](/docs/components/processors/llm) prompt or downstream
JSON tooling.

:::note
Failures (null/empty query vector, SQL errors) fail the batch into
`error_output`. The processor does not retry. The connection pool is
created lazily, so building the component never touches the network.
:::

## Example

```yaml validate=fragment wrap=processors
- type: "pgvector_search"
  url: "postgres://postgres:${env:PG_PASSWORD:-}@localhost:5432/vectors"
  table: "documents"
  metric: "cosine"
  top_k: 5
```

### Complete RAG Query Pipeline

```yaml validate=full
logging:
  level: info

streams:
  - id: pgvector-rag-query
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
        - type: pgvector_search
          url: "postgres://postgres:${env:PG_PASSWORD:-}@localhost:5432/vectors"
          table: "documents"
          metric: "cosine"
          top_k: 5
    output:
      type: stdout
```

## Testing against a live database

The workspace test suite contains an ignored integration test that needs a
real Postgres with the pgvector extension:

```bash
docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=postgres pgvector/pgvector:pg16
cargo test -p arkflow-plugin --lib processor::pgvector_search -- --ignored
```
