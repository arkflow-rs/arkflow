---
components: [vector_search]
description: ArkFlow documentation page.
---

# Vector Search

The `vector_search` processor searches a [Qdrant](https://qdrant.tech/) collection for the top-k nearest neighbors of each row's vector and appends the matches as a JSON array text column. It is the retrieval step of an in-engine RAG pipeline: pair it with the [embedding processor](/docs/components/2-processors/embedding) (turn text into a query vector) and the [LLM processor](/docs/components/2-processors/llm) (generate over the matches).

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `vector_search` |
| url | string | yes | — | Qdrant base URL (e.g. `http://localhost:6333`). |
| collection | string | yes | — | Collection to search. |
| vector_field | string | no | `embedding` | Column holding the query vector (`FixedSizeList`/`List` of Float32). |
| target_field | string | no | `matches` | Name of the appended matches column. |
| top_k | integer | no | `5` | Number of neighbors per row. |
| score_threshold | number | no | — | Minimum similarity score; omitted from the request when unset. |
| concurrency | integer | no | `4` | Maximum in-flight requests; results are always filled back in row order. |
| api_key | string | no | — | Sent as `Authorization: Bearer`. Supports [secret references](/docs/reference/configuration#secret-references). |
| timeout_ms | integer | no | `30000` | HTTP request timeout. |
| headers | map | no | — | Extra HTTP headers. |

## Result shape

The appended column is a JSON array of matches, ordered by descending
score, each preserving Qdrant's `id`, `score`, and `payload`:

```json
[{"id": 42, "score": 0.93, "payload": {"text": "..."}}]
```

Each row is one search request (bounded by `concurrency`); the JSON text
feeds directly into an [LLM processor](/docs/components/2-processors/llm)
`prompt_template` via `{{value}}`-style composition over the column, or
into any downstream JSON tooling.

:::note
Failures (non-2xx, missing result array, null/empty query vector) fail the
batch into `error_output`. The processor does not retry; size
`concurrency` to stay within rate limits. Requests to loopback endpoints
always bypass the system proxy.
:::

## Example

```yaml validate=fragment wrap=processors
- type: "vector_search"
  url: "http://localhost:6333"
  collection: "documents"
  vector_field: "embedding"
  top_k: 5
```

### Complete RAG Query Pipeline

Query text → embedding → top-k retrieval → LLM answer, all in one stream:

```yaml validate=full
logging:
  level: info

streams:
  - id: rag-query
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
        - type: vector_search
          url: "http://localhost:6333"
          collection: "documents"
          top_k: 5
          target_field: "matches"
        - type: sql
          query: "SELECT *, concat('Question: ', text, ' | Passages: ', matches) AS rag_prompt FROM flow"
        - type: llm
          api_base: "https://api.openai.com/v1"
          model: "gpt-4o-mini"
          api_key: "${env:OPENAI_API_KEY:-}"
          field: "rag_prompt"
          system_prompt: "Answer the user's question using only the provided passages."
          target_field: "answer"
    output:
      type: stdout
```
