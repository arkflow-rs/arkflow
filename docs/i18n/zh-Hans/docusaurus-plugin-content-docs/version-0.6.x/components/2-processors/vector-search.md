---
components: [vector_search]
description: ArkFlow Vector Search 处理器组件。
---

# Vector Search

`vector_search` 处理器(processor)在 [Qdrant](https://qdrant.tech/) 集合(Collection)中检索每行向量的 top-k 最近邻,并把匹配结果追加为 JSON 数组文本列。它是引擎内 RAG 管道的检索步骤:与 [embedding 处理器](/zh-Hans/docs/components/processors/embedding)(把文本变成查询向量)以及 [LLM 处理器](/zh-Hans/docs/components/processors/llm)(基于匹配结果生成)搭配使用。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `vector_search` |
| url | string | yes | — | Qdrant 基础 URL(如 `http://localhost:6333`)。 |
| collection | string | yes | — | 要检索的集合。 |
| vector_field | string | no | `embedding` | 存放查询向量的列(Float32 的 `FixedSizeList`/`List`)。 |
| target_field | string | no | `matches` | 追加的匹配结果列名称。 |
| top_k | integer | no | `5` | 每行的邻居数量。 |
| score_threshold | number | no | — | 最小相似度得分;未设置时不随请求发送。 |
| concurrency | integer | no | `4` | 最大并发请求数;结果始终按行序回填。 |
| api_key | string | no | — | 以 `Authorization: Bearer` 形式发送。支持 [Secret 引用](/zh-Hans/docs/reference/configuration#secret-references)。 |
| timeout_ms | integer | no | `30000` | HTTP 请求超时。 |
| headers | map | no | — | 额外的 HTTP 头。 |

## 结果形状

追加的列是一个由匹配结果组成的 JSON 数组,按得分降序排列,每个元素保留
Qdrant 的 `id`、`score` 与 `payload`:

```json
[{"id": 42, "score": 0.93, "payload": {"text": "..."}}]
```

每一行对应一个检索请求(受 `concurrency` 约束);该 JSON 文本可通过对该列做
`{{value}}` 式拼接,直接喂给 [LLM 处理器](/zh-Hans/docs/components/processors/llm)
的 `prompt_template`,也可交给任何下游 JSON 工具。

:::note
失败(非 2xx、缺少结果数组、查询向量为 null 或空)会使整个批次(Batch)
失败并流向 `error_output`。该处理器不会重试;请合理设置 `concurrency`
以保持在速率限制之内。发往回环(loopback)端点的请求始终绕过系统代理。
:::

## 示例

```yaml validate=fragment wrap=processors
- type: "vector_search"
  url: "http://localhost:6333"
  collection: "documents"
  vector_field: "embedding"
  top_k: 5
```

### 完整 RAG 查询管道

查询文本 → 向量化 → top-k 检索 → LLM 回答,全部在一条流内完成:

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
