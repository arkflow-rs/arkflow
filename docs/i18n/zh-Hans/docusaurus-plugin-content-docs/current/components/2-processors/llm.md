---
components: [llm]
description: ArkFlow LLM 处理器组件。
---

# LLM

`llm` 处理器(processor)将文本列的每一行发送到 OpenAI 兼容的 chat completions API,并把补全结果追加为 Utf8 列。可用它在流内完成摘要、翻译、改写、分类或结构化抽取。任何支持 OpenAI `/chat/completions` 协议的端点均可使用:OpenAI、Azure OpenAI(通过 `headers`)、vLLM 或 Ollama 网关。

它天然适合与 [SQL](/zh-Hans/docs/components/processors/sql)(先构造提示词列)以及 [embedding 处理器](/zh-Hans/docs/components/processors/embedding)(对模型产出做向量化)搭配使用。

## 配置

| Field | Type | Required | Default | 描述 |
|-------|------|----------|---------|-------------|
| type | string | yes | — | `llm` |
| api_base | string | yes | — | API 基础 URL;请求路径为 `{api_base}/chat/completions`。 |
| model | string | yes | — | 聊天模型名称(如 `gpt-4o-mini`)。 |
| field | string | yes | — | 待发送的输入 UTF-8 列。各行必须非 null。 |
| target_field | string | no | `response` | 追加的补全结果列名称。 |
| system_prompt | string | no | — | 前置到每个请求的系统消息。 |
| prompt_template | string | no | — | 用户消息模板;`{{value}}` 会被替换为该行文本。省略时行文本即用户消息。 |
| temperature | number | no | — | 采样温度;未设置时不随请求发送。 |
| max_tokens | integer | no | — | 补全 token 上限;未设置时不随请求发送。 |
| concurrency | integer | no | `4` | 最大并发请求数;结果始终按行序回填。 |
| api_key | string | no | — | 以 `Authorization: Bearer` 形式发送。支持 [Secret 引用](/zh-Hans/docs/reference/configuration#secret-references)。 |
| timeout_ms | integer | no | `30000` | HTTP 请求超时。 |
| headers | map | no | — | 额外的 HTTP 头(如 Azure 的 `api-key`)。 |
| stream | boolean | no | `false` | 请求 SSE 流式传输,并将增量(delta)组装为该行的最终补全文本。适用于会对长补全的非流式请求超时的提供商或网关。处理器仍为每行输出一段完整文本。 |
| tools | array | no | — | 原样透传的 OpenAI [tools 定义](https://platform.openai.com/docs/guides/function-calling),用于函数调用。 |
| tool_calls_column | string | no | — | 设置后追加该列,以 JSON 文本填充响应中的工具调用(没有时为空字符串)。流式工具调用增量按索引合并。 |

:::note
每一行对应一个请求(聊天协议没有批量形式)。第一行失败即短路整个批次——飞行中的请求会被丢弃而非等待完成——整个批次(Batch)作为整体流向 `error_output`。该处理器不会重试 LLM 调用;请合理设置 `concurrency` 与上游批次大小,以保持在速率限制之内。发往回环(loopback)端点的请求始终绕过系统代理。
:::

## 示例

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

### 完整管道示例

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
