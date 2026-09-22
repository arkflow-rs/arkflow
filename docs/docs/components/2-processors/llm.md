---
components: [llm]
description: ArkFlow documentation page.
---

# LLM

The `llm` processor sends each row of a text column to an OpenAI-compatible chat completions API and appends the completion as a Utf8 column. Use it for in-stream summarization, translation, rewriting, classification, or structured extraction. Any endpoint that speaks the OpenAI `/chat/completions` protocol works: OpenAI, Azure OpenAI (via `headers`), vLLM, or Ollama gateways.

It pairs naturally with [SQL](/docs/components/2-processors/sql) (prepare the prompt column first) and the [embedding processor](embedding.md) (embed what the model produced).

## Configuration

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `llm` |
| api_base | string | yes | — | API base URL; the request path is `{api_base}/chat/completions`. |
| model | string | yes | — | Chat model name (e.g. `gpt-4o-mini`). |
| field | string | yes | — | Input UTF-8 column to send. Rows must be non-null. |
| target_field | string | no | `response` | Name of the appended completion column. |
| system_prompt | string | no | — | System message prepended to each request. |
| prompt_template | string | no | — | User-message template; `{{value}}` is replaced with the row text. When omitted the row text is the user message. |
| temperature | number | no | — | Sampling temperature; omitted from the request when unset. |
| max_tokens | integer | no | — | Completion token cap; omitted from the request when unset. |
| concurrency | integer | no | `4` | Maximum in-flight requests; results are always filled back in row order. |
| api_key | string | no | — | Sent as `Authorization: Bearer`. Supports [secret references](/docs/reference/configuration#secret-references). |
| timeout_ms | integer | no | `30000` | HTTP request timeout. |
| headers | map | no | — | Extra HTTP headers (e.g. Azure's `api-key`). |

:::note
Each row is one request (the chat protocol has no batch form). Requests
already in flight complete even if an earlier row fails — the batch fails
as a whole and flows to `error_output`. The processor does not retry
LLM calls; size `concurrency` and upstream batches to stay within rate
limits. Requests to loopback endpoints always bypass the system proxy.
:::

## Example

```yaml validate=fragment wrap=processors
- type: "llm"
  api_base: "https://api.openai.com/v1"
  model: "gpt-4o-mini"
  api_key: "${env:OPENAI_API_KEY:-}"
  field: "text"
  system_prompt: "Translate the text to French. Reply with the translation only."
  prompt_template: "{{value}}"
  concurrency: 4
```

### Complete Pipeline Example

```yaml validate=full
logging:
  level: info

streams:
  - id: support-tagger
    input:
      type: memory
    pipeline:
      thread_num: 4
      processors:
        - type: json_to_arrow
        - type: llm
          api_base: "https://api.openai.com/v1"
          model: "gpt-4o-mini"
          api_key: "${env:OPENAI_API_KEY:-}"
          field: "text"
          system_prompt: "Classify the support ticket with a single word: billing, bug, or feature."
          target_field: "category"
          temperature: 0
    output:
      type: stdout
```
