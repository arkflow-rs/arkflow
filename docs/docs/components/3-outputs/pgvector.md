---
components: [pgvector]
description: ArkFlow documentation page.
---

# pgvector

The `pgvector` output upserts each batch's rows into a PostgreSQL table with the [pgvector](https://github.com/pgvector/pgvector) extension. It pairs with the [embedding processor](/docs/components/2-processors/embedding) so an existing Postgres becomes the vector store — no separate vector database deployment.

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `pgvector` |
| url | string | yes | — | Postgres connection string. Supports [secret references](/docs/reference/configuration#secret-references). |
| table | string | yes | — | Target table (must already exist). |
| vector_field | string | no | `embedding` | Column holding the vector (`FixedSizeList`/`List` of Float32). |
| id_field | string | no | — | Column used as the upsert conflict key (integer or string). When omitted, plain INSERTs are written. |
| payload_field | string | no | `payload` | jsonb column receiving every remaining column as a per-row JSON object. Set to `""` to disable. |
| max_connections | integer | no | `4` | Connection pool size. |
| timeout_ms | integer | no | `30000` | Pool acquire/connect timeout. |

## Table and SQL shape

The table schema is managed by you. For the example below:

```sql
CREATE TABLE documents (
  doc_id BIGINT PRIMARY KEY,
  embedding vector(768),
  payload jsonb
);
```

Each batch produces one parameterized statement; vectors are bound as text
with an explicit `::vector` cast and the payload as `::jsonb`:

```sql
INSERT INTO "documents" ("doc_id", "embedding", "payload")
VALUES ($1, $2::vector, $3::jsonb)
ON CONFLICT ("doc_id") DO UPDATE
SET "embedding" = EXCLUDED."embedding", "payload" = EXCLUDED."payload"
```

:::note
With `id_field` set, the target column must carry a unique/PK constraint —
redelivery overwrites the same row (at-least-once friendly). Without it,
every attempt appends a new row. Dimension mismatches between the stream
and the table's `vector(n)` column fail with the Postgres error surfaced.
:::

## Example

```yaml validate=fragment wrap=output
output:
  type: "pgvector"
  url: "postgres://postgres:${env:PG_PASSWORD:-}@localhost:5432/vectors"
  table: "documents"
  vector_field: "embedding"
  id_field: "doc_id"
```

### Complete Pipeline Example

```yaml validate=full
logging:
  level: info

streams:
  - id: docs-to-postgres
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
      type: pgvector
      url: "postgres://postgres:${env:PG_PASSWORD:-}@localhost:5432/vectors"
      table: "documents"
      vector_field: "embedding"
      id_field: "doc_id"
```

## Testing against a live database

The workspace test suite contains an ignored integration test that needs a
real Postgres with the pgvector extension:

```bash
docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=postgres pgvector/pgvector:pg16
cargo test -p arkflow-plugin --lib output::pgvector -- --ignored
```
